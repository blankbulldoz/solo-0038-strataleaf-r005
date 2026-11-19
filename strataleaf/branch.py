"""Independent writable data branches with common-base three-way merge.

A branch is a lightweight fork of the primary database at a historical revision.
Branch writes live in their own revision namespace and never touch primary document
state.  Merging compares three views -- the common base (``branch.base``), the
current primary head and the branch head -- so that changes made on both sides are
detected per document key instead of overwriting the primary blindly.  A merge
either reports inspectable conflicts or lands as one ordinary, traceable commit.
"""
import math
import uuid

from .codec import digest, dumps, field, key as checked_key, loads, name, scalar_token
from .errors import Conflict, InvalidDocument, MergeConflict, NotFound, SnapshotTooOld
from .mvcc import Record, checked_revision, latest_row, visible_rows
from .schema import normalize_schema, validate_document
from .transaction import (CommitResult, apply_operations, check_expected, record_commit,
                          verify_operations)

ACTIONS = {"take_branch", "take_main", "take_document", "delete"}


def _branch_row(connection, branch):
    row = connection.execute("SELECT * FROM branches WHERE name=?", (branch,)).fetchone()
    if row is None:
        raise NotFound("unknown branch: " + branch)
    return row


def _info(row):
    return {"name": row["name"], "token": row["token"], "base": row["base"],
            "head_rev": row["head_rev"], "synced": loads(row["synced"]) if row["synced"] else {},
            "sealed": bool(row["sealed"]), "sealed_digest": row["sealed_digest"],
            "applied_rev": row["applied_rev"]}


def _overlay_rows(connection, branch, revision):
    return {(row["collection"], row["key"]): row for row in connection.execute(
        "SELECT * FROM branch_versions WHERE branch=? AND rev<=? ORDER BY rev", (branch, revision))}


def _main_view(connection, collections, revision):
    output = {}
    for collection in collections:
        for row in visible_rows(connection, collection, revision):
            output[(collection, row["key"])] = {
                "document": loads(row["body"]), "expires_at": row["expires"], "revision": row["rev"]}
    return output


def _branch_view(connection, branch, revision, collections, base):
    overlay = _overlay_rows(connection, branch, revision)
    output = _main_view(connection, collections, base)
    for identity, row in overlay.items():
        if identity[0] not in collections:
            continue
        if row["body"] is None:
            output.pop(identity, None)
        else:
            output[identity] = {"document": loads(row["body"]), "expires_at": row["expires"],
                                "revision": row["rev"]}
    return output


def _same(left, right):
    if left is None or right is None:
        return left is None and right is None
    return dumps(left["document"]) == dumps(right["document"]) and left["expires_at"] == right["expires_at"]


def _normalize_collections(connection, collections):
    if collections is None:
        return sorted(row[0] for row in connection.execute("SELECT name FROM collections"))
    if isinstance(collections, str) or not isinstance(collections, (list, tuple)) or not collections:
        raise InvalidDocument("collections must be a nonempty list of collection names")
    output = []
    for collection in collections:
        name(collection)
        if collection in output:
            raise InvalidDocument("duplicate collection in merge selection: " + collection)
        if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
            raise NotFound("unknown collection: " + collection)
        output.append(collection)
    return output


def _normalize_resolutions(resolutions):
    if resolutions is None:
        return {}
    if isinstance(resolutions, dict) or not isinstance(resolutions, (list, tuple)):
        raise InvalidDocument("resolutions must be a list of per-document actions")
    normalized = {}
    for item in resolutions:
        if not isinstance(item, dict) or set(item) - {"collection", "key", "action", "document"}:
            raise InvalidDocument("resolution requires collection, key and action")
        collection, key, action = item.get("collection"), item.get("key"), item.get("action")
        name(collection)
        checked_key(key)
        if action not in ACTIONS:
            raise InvalidDocument("resolution action must be one of " + ", ".join(sorted(ACTIONS)))
        if action == "take_document" and not isinstance(item.get("document"), dict):
            raise InvalidDocument("take_document resolution requires a document object")
        if action != "take_document" and "document" in item:
            raise InvalidDocument("only take_document accepts a document")
        identity = (collection, key)
        if identity in normalized:
            raise InvalidDocument("duplicate resolution for " + collection + "/" + key)
        normalized[identity] = {"action": action, "document": loads(dumps(item["document"])) if action == "take_document" else None,
                                "used": False}
    return normalized


class BranchWriteBatch:
    """Write batch scoped to one branch.  Primary revisions are never allocated."""

    def __init__(self, store, branch, request_key=None):
        self.store = store
        self.branch = name(branch)
        self.request_key = checked_key(request_key) if request_key is not None else None
        self.operations = []
        self.state = "open"

    def _append(self, operation):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        identity = (operation["collection"], operation["key"])
        if any((op["collection"], op["key"]) == identity for op in self.operations):
            raise InvalidDocument("one operation per document per batch")
        check_expected(operation["expected"])
        self.operations.append(operation)
        return self

    def put(self, collection, key, document, expected=None, expires_at=None):
        name(collection)
        checked_key(key)
        if not isinstance(document, dict):
            raise InvalidDocument("document must be an object")
        if expires_at is not None and (isinstance(expires_at, bool) or not isinstance(expires_at, (int, float))
                                      or not math.isfinite(expires_at)):
            raise InvalidDocument("expires_at must be a finite timestamp")
        return self._append({"collection": collection, "key": key, "document": loads(dumps(document)),
                             "expected": expected, "expires_at": expires_at})

    def delete(self, collection, key, expected=None):
        return self._append({"collection": name(collection), "key": checked_key(key), "document": None,
                             "expected": expected, "expires_at": None})

    def abort(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        self.operations.clear()
        self.state = "aborted"

    def commit(self):
        if self.state != "open":
            raise Conflict("batch is already " + self.state)
        if not self.operations or len(self.operations) > 10000:
            raise InvalidDocument("batch must contain between 1 and 10000 operations")
        fingerprint = digest(self.operations)
        try:
            with self.store.atomic(writable=True) as connection:
                row = _branch_row(connection, self.branch)
                if row["sealed"]:
                    raise Conflict("branch is sealed and rejects further writes")
                if self.request_key is not None:
                    prior = connection.execute(
                        "SELECT * FROM branch_receipts WHERE branch=? AND request_key=?",
                        (self.branch, self.request_key)).fetchone()
                    if prior:
                        if prior["request_hash"] != fingerprint:
                            raise Conflict("request key was used with different branch operations")
                        self.state = "committed"
                        return CommitResult(prior["rev"], prior["count"], True)
                head = row["head_rev"]
                for op in self.operations:
                    catalog = connection.execute(
                        "SELECT schema FROM branch_catalog WHERE branch=? AND name=?",
                        (self.branch, op["collection"])).fetchone()
                    if catalog is None:
                        raise NotFound("unknown branch collection: " + op["collection"])
                    latest = connection.execute(
                        "SELECT * FROM branch_versions WHERE branch=? AND collection=? AND key=? AND rev<=?"
                        " ORDER BY rev DESC LIMIT 1", (self.branch, op["collection"], op["key"], head)).fetchone()
                    if latest is not None:
                        current = latest["rev"]
                    else:
                        base_row = latest_row(connection, op["collection"], op["key"], row["base"])
                        current = base_row["rev"] if base_row is not None else 0
                    expected = op["expected"]
                    if expected is not None and expected != current:
                        raise Conflict(f"revision conflict for {op['collection']}/{op['key']}: "
                                       f"expected {expected}, found {current}")
                    if op["document"] is not None:
                        validate_document(op["document"], loads(catalog[0]))
                revision = head + 1
                for op in self.operations:
                    body = dumps(op["document"]) if op["document"] is not None else None
                    connection.execute("INSERT INTO branch_versions VALUES (?,?,?,?,?,?)",
                                       (self.branch, op["collection"], op["key"], revision, body,
                                        op["expires_at"]))
                payload = {"revision": revision, "operations": self.operations}
                connection.execute("INSERT INTO branch_commits VALUES (?,?,?,?,?)",
                                   (self.branch, revision, dumps(payload), self.request_key, fingerprint))
                if self.request_key is not None:
                    connection.execute("INSERT INTO branch_receipts VALUES (?,?,?,?,?)",
                                       (self.branch, self.request_key, revision, fingerprint, len(self.operations)))
                connection.execute("UPDATE branches SET head_rev=? WHERE name=?", (revision, self.branch))
        except BaseException:
            self.state = "aborted"
            raise
        self.state = "committed"
        return CommitResult(revision, len(self.operations))


class Branches:
    def __init__(self, store):
        self.store = store

    # -- lifecycle -------------------------------------------------------
    def create(self, branch, from_revision):
        branch = name(branch)
        if from_revision is None:
            raise InvalidDocument("branch creation requires an explicit base revision")
        with self.store.atomic(writable=True) as connection:
            revision = checked_revision(self.store, from_revision)
            if connection.execute("SELECT 1 FROM branches WHERE name=?", (branch,)).fetchone():
                raise Conflict("branch already exists: " + branch)
            connection.execute("INSERT INTO branches VALUES (?,?,?,?,?,?,?,?)",
                               (branch, uuid.uuid4().hex, revision, revision, None, None, 0, None))
            connection.executemany(
                "INSERT INTO branch_catalog VALUES (?,?,?,?)",
                [(branch, row["name"], row["schema"], row["generation"])
                 for row in connection.execute("SELECT * FROM collections")])
        return self.describe(branch)

    def describe(self, branch):
        with self.store.read() as connection:
            return _info(_branch_row(connection, branch))

    def list(self):
        with self.store.read() as connection:
            return [_info(row) for row in connection.execute("SELECT * FROM branches ORDER BY name")]

    def clone(self, source, branch, from_revision=None):
        """Create ``branch`` from the state of another branch at ``from_revision``."""
        source, branch = name(source), name(branch)
        with self.store.atomic(writable=True) as connection:
            row = _branch_row(connection, source)
            revision = row["head_rev"] if from_revision is None else self._branch_revision(row, from_revision)
            if connection.execute("SELECT 1 FROM branches WHERE name=?", (branch,)).fetchone():
                raise Conflict("branch already exists: " + branch)
            inherited = loads(row["synced"]) if row["synced"] else {}
            inherited = {key: min(value, revision) for key, value in inherited.items()}
            connection.execute("INSERT INTO branches VALUES (?,?,?,?,?,?,?,?)",
                               (branch, uuid.uuid4().hex, row["base"], revision,
                                dumps(inherited) if inherited else None, None, 0, None))
            connection.executemany(
                "INSERT INTO branch_catalog VALUES (?,?,?,?)",
                [(branch, item["name"], item["schema"], item["generation"])
                 for item in connection.execute("SELECT * FROM branch_catalog WHERE branch=?", (source,))])
            connection.executemany(
                "INSERT INTO branch_versions VALUES (?,?,?,?,?,?)",
                [(branch, item["collection"], item["key"], item["rev"], item["body"], item["expires"])
                 for item in connection.execute(
                     "SELECT * FROM branch_versions WHERE branch=? AND rev<=?", (source, revision))])
        return self.describe(branch)

    def refresh_catalog(self, branch, schemas=None):
        """Reconcile the branch's private catalog with the primary catalog.

        Without ``schemas`` the current primary schemas are adopted (for example after
        the primary catalog evolves); ``schemas`` maps collection name to an explicit
        ``{"fields", "required", "additional"}`` definition and must reference an
        existing primary collection.  Existing branch documents are validated first,
        so an incompatible change fails without altering the branch catalog.
        """
        branch = name(branch)
        if schemas is not None and not isinstance(schemas, dict):
            raise InvalidDocument("branch catalog update must be a mapping of collection schemas")
        with self.store.atomic(writable=True) as connection:
            row = _branch_row(connection, branch)
            if row["sealed"]:
                raise Conflict("branch is sealed and its catalog cannot change")
            changes = {}
            if schemas is None:
                for item in connection.execute("SELECT * FROM collections"):
                    changes[item["name"]] = loads(item["schema"])
            else:
                for collection, schema in schemas.items():
                    name(collection)
                    changes[collection] = normalize_schema(schema)
                    if not connection.execute("SELECT 1 FROM collections WHERE name=?", (collection,)).fetchone():
                        raise NotFound("unknown collection: " + collection)
            for collection, schema in changes.items():
                for document in self._branch_records(connection, branch, collection):
                    validate_document(document, schema)
            for collection, schema in changes.items():
                existing = connection.execute(
                    "SELECT generation FROM branch_catalog WHERE branch=? AND name=?",
                    (branch, collection)).fetchone()
                generation = (existing["generation"] + 1) if existing else 1
                connection.execute(
                    "INSERT INTO branch_catalog VALUES (?,?,?,?) ON CONFLICT(branch,name) DO UPDATE SET"
                    " schema=excluded.schema, generation=excluded.generation",
                    (branch, collection, dumps(schema), generation))
            return [{"name": item["name"], "schema": loads(item["schema"]), "generation": item["generation"]}
                    for item in connection.execute(
                        "SELECT * FROM branch_catalog WHERE branch=? ORDER BY name", (branch,))]

    def _branch_records(self, connection, branch, collection):
        row = _branch_row(connection, branch)
        view = _branch_view(connection, branch, row["head_rev"], [collection], row["base"])
        return [item["document"] for item in view.values()]

    def drop(self, branch):
        branch = name(branch)
        with self.store.atomic(writable=True) as connection:
            _branch_row(connection, branch)
            for table in ("branch_catalog", "branch_versions", "branch_commits", "branch_receipts"):
                connection.execute("DELETE FROM " + table + " WHERE branch=?", (branch,))
            connection.execute("DELETE FROM branches WHERE name=?", (branch,))

    def batch(self, branch, request_key=None):
        return BranchWriteBatch(self.store, branch, request_key)

    def seal(self, branch):
        branch = name(branch)
        with self.store.atomic(writable=True) as connection:
            row = _branch_row(connection, branch)
            collections = [item[0] for item in connection.execute(
                "SELECT name FROM branch_catalog WHERE branch=? ORDER BY name", (branch,))]
            view = _branch_view(connection, branch, row["head_rev"], collections, row["base"])
            snapshot = [{"collection": identity[0], "key": identity[1],
                         "revision": item["revision"], "document": item["document"],
                         "expires_at": item["expires_at"]}
                        for identity, item in sorted(view.items())]
            fingerprint = digest({"branch": branch, "base": row["base"],
                                  "head": row["head_rev"], "documents": snapshot})
            if not row["sealed"]:
                connection.execute("UPDATE branches SET sealed=1, sealed_digest=? WHERE name=?",
                                   (fingerprint, branch))
            return {"name": branch, "token": row["token"], "base": row["base"],
                    "head_rev": row["head_rev"], "sealed": True,
                    "digest": row["sealed_digest"] if row["sealed"] else fingerprint,
                    "documents": len(snapshot)}

    # -- reads -----------------------------------------------------------
    def get(self, branch, collection, key, revision=None):
        branch = name(branch)
        checked_key(key)
        with self.store.read() as connection:
            row = _branch_row(connection, branch)
            if not connection.execute("SELECT 1 FROM branch_catalog WHERE branch=? AND name=?",
                                      (branch, collection)).fetchone():
                raise NotFound("unknown branch collection: " + collection)
            revision = self._branch_revision(row, revision)
            latest = connection.execute(
                "SELECT * FROM branch_versions WHERE branch=? AND collection=? AND key=? AND rev<=?"
                " ORDER BY rev DESC LIMIT 1", (branch, collection, key, revision)).fetchone()
            if latest is not None:
                if latest["body"] is None:
                    return None
                return Record(collection, key, latest["rev"], loads(latest["body"]), latest["expires"])
            base_row = latest_row(connection, collection, key, row["base"])
            if base_row is None or base_row["body"] is None:
                return None
            return Record(collection, key, base_row["rev"], loads(base_row["body"]), base_row["expires"])

    def scan(self, branch, collection, revision=None):
        branch = name(branch)
        with self.store.read() as connection:
            row = _branch_row(connection, branch)
            if not connection.execute("SELECT 1 FROM branch_catalog WHERE branch=? AND name=?",
                                      (branch, collection)).fetchone():
                raise NotFound("unknown branch collection: " + collection)
            revision = self._branch_revision(row, revision)
            view = _branch_view(connection, branch, revision, [collection], row["base"])
            return [Record(collection, identity[1], item["revision"], item["document"], item["expires_at"])
                    for identity, item in sorted(view.items())]

    @staticmethod
    def _branch_revision(row, revision):
        if revision is None:
            return row["head_rev"]
        if isinstance(revision, bool) or not isinstance(revision, int) or revision < row["base"] or revision > row["head_rev"]:
            raise InvalidDocument("branch revision must be between its base and current head")
        return revision

    def commits(self, branch, since=None, limit=100):
        branch = name(branch)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("branch commit limit must be between 1 and 1000")
        with self.store.read() as connection:
            row = _branch_row(connection, branch)
            since = row["base"] - 1 if since is None else since
            if isinstance(since, bool) or not isinstance(since, int) or since < row["base"] - 1:
                raise InvalidDocument("branch commit cursor is outside the branch")
            return [loads(item[0]) for item in connection.execute(
                "SELECT payload FROM branch_commits WHERE branch=? AND rev>? ORDER BY rev LIMIT ?",
                (branch, since, limit))]

    # -- merge -----------------------------------------------------------
    def preview(self, branch, collections=None, resolutions=None):
        branch = name(branch)
        with self.store.read() as connection:
            row = _branch_row(connection, branch)
            selected = _normalize_collections(connection, collections)
            actions = _normalize_resolutions(resolutions)
            conflicts, plan = self._plan(connection, row, row["head_rev"], selected, actions)
            self._reject_unused(actions, conflicts, plan)
            return {"branch": branch, "base": row["base"], "branch_revision": row["head_rev"],
                    "main_revision": int(self.store.get("head")), "sealed": bool(row["sealed"]),
                    "conflicts": conflicts,
                    "operations": [self._public_operation(item) for item in plan],
                    "mergeable": not conflicts}

    def merge(self, branch, request_key=None, collections=None, resolutions=None):
        branch = name(branch)
        request_key = checked_key(request_key) if request_key is not None else None
        with self.store.atomic(writable=True) as connection:
            row = _branch_row(connection, branch)
            selected = _normalize_collections(connection, collections)
            actions = _normalize_resolutions(resolutions)
            scope = digest({"branch": branch, "token": row["token"], "branch_revision": row["head_rev"],
                            "collections": selected})
            fingerprint = digest({"scope": scope, "base": row["base"], "resolutions": [
                {"collection": identity[0], "key": identity[1], "action": action["action"],
                 "document": action["document"]} for identity, action in sorted(actions.items())]})
            if request_key is not None:
                prior = connection.execute("SELECT * FROM receipts WHERE request_key=?", (request_key,)).fetchone()
                if prior:
                    if prior["request_hash"] != fingerprint:
                        raise Conflict("request key was used for a different merge")
                    return {"branch": branch, "base": row["base"], "branch_revision": row["head_rev"],
                            "revision": prior["rev"], "applied": 0, "replayed": True,
                            "already_applied": True, "conflicts": []}
            prior_merge = connection.execute(
                "SELECT * FROM branch_merges WHERE branch=? AND token=? AND branch_rev=? AND scope=?"
                " AND outcome='applied'", (branch, row["token"], row["head_rev"], scope)).fetchone()
            if prior_merge:
                if prior_merge["request_hash"] != fingerprint:
                    raise Conflict("branch revision was already merged with a different resolution set")
                if request_key is not None:
                    connection.execute("INSERT OR REPLACE INTO receipts VALUES (?,?,?,?)",
                                       (request_key, prior_merge["revision"], fingerprint, 0))
                return {"branch": branch, "base": row["base"], "branch_revision": row["head_rev"],
                        "revision": prior_merge["revision"], "applied": 0, "replayed": True,
                        "already_applied": True, "conflicts": []}
            head = int(self.store.get("head"))
            conflicts, plan = self._plan(connection, row, row["head_rev"], selected, actions)
            self._reject_unused(actions, conflicts, plan)
            if conflicts:
                raise MergeConflict("branch merge requires explicit conflict resolution", conflicts,
                                    {"branch": branch, "base": row["base"], "branch_revision": row["head_rev"],
                                     "main_revision": head,
                                     "operations": [self._public_operation(item) for item in plan]})
            if not plan:
                connection.execute(
                    "INSERT INTO branch_merges VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (branch, row["token"], row["head_rev"], scope, request_key or "", fingerprint, head,
                     "up_to_date", 0, row["base"]))
                self._mark_synced(connection, row, selected)
                if request_key is not None:
                    connection.execute("INSERT INTO receipts VALUES (?,?,?,?)",
                                       (request_key, head, fingerprint, 0))
                return {"branch": branch, "base": row["base"], "branch_revision": row["head_rev"],
                        "revision": head, "applied": 0, "replayed": False, "already_applied": False,
                        "conflicts": [], "operations": 0}
            normalized = []
            for item in plan:
                verify_operations(connection, head, [item])
                normalized.append(item)
            revision = head + 1
            apply_operations(connection, revision, normalized)
            merge_meta = {"branch": branch, "base": row["base"], "branch_revision": row["head_rev"]}
            record_commit(connection, revision, normalized, request_key, fingerprint, merge_meta)
            connection.execute(
                "INSERT INTO branch_merges VALUES (?,?,?,?,?,?,?,?,?,?)",
                (branch, row["token"], row["head_rev"], scope, request_key or "", fingerprint, revision,
                 "applied", len(normalized), row["base"]))
            connection.execute("UPDATE branches SET applied_rev=? WHERE name=?",
                               (row["head_rev"], branch))
            self._mark_synced(connection, row, selected)
            self.store.set("head", revision)
            return {"branch": branch, "base": row["base"], "branch_revision": row["head_rev"],
                    "revision": revision, "applied": len(normalized), "replayed": False,
                    "already_applied": False, "conflicts": [], "operations": len(normalized)}

    def merges(self, branch=None, limit=100):
        if branch is not None:
            name(branch)
        if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
            raise InvalidDocument("merge history limit must be between 1 and 1000")
        with self.store.read() as connection:
            rows = connection.execute(
                "SELECT * FROM branch_merges WHERE (? IS NULL OR branch=?) ORDER BY revision, branch LIMIT ?",
                (branch, branch, limit)).fetchall()
            return [dict(row) for row in rows]

    # -- planning --------------------------------------------------------
    def _plan(self, connection, row, revision, collections, actions):
        branch, base, head = row["name"], row["base"], int(self.store.get("head"))
        if base < int(self.store.get("floor")):
            raise SnapshotTooOld("branch base revision was reclaimed before merge")
        sync = loads(row["synced"]) if row["synced"] else {}
        base_view = {}
        for collection in collections:
            if collection in sync:
                base_view.update(_branch_view(connection, branch, sync[collection], [collection], base))
            else:
                base_view.update(_main_view(connection, [collection], base))
        branch_view = _branch_view(connection, branch, revision, collections, base)
        main_view = _main_view(connection, collections, head)
        identities = sorted(set(base_view) | set(branch_view) | set(main_view))
        conflicts = []
        plan = []
        for identity in identities:
            collection, key = identity
            before = base_view.get(identity)
            theirs = branch_view.get(identity)
            ours = main_view.get(identity)
            if _same(theirs, before):
                continue
            if _same(theirs, ours):
                continue
            action = actions.get(identity)
            if _same(ours, before):
                document = theirs["document"] if theirs is not None else None
                expires = theirs["expires_at"] if theirs is not None else None
                plan.append(self._operation(connection, collection, key, document, expires, head))
                continue
            if action is None:
                conflicts.append(self._conflict(collection, key, before, ours, theirs, connection, head))
                continue
            action["used"] = True
            if action["action"] == "take_main":
                continue
            document, selected = _resolve(action, theirs)
            plan.append(self._operation(connection, collection, key, document, selected, head))
        if conflicts:
            return conflicts, plan
        return self._validate(connection, head, plan, actions, collections)

    @staticmethod
    def _mark_synced(connection, row, collections):
        sync = loads(row["synced"]) if row["synced"] else {}
        for collection in collections:
            sync[collection] = row["head_rev"]
        connection.execute("UPDATE branches SET synced=? WHERE name=?", (dumps(sync), row["name"]))

    def _operation(self, connection, collection, key, document, expires_at, head):
        current = latest_row(connection, collection, key, head)
        return {"collection": collection, "key": key,
                "document": loads(dumps(document)) if document is not None else None,
                "expires_at": expires_at, "expected": current["rev"] if current is not None else 0}

    def _conflict(self, collection, key, before, ours, theirs, connection, head):
        if before is None:
            kind = "add_vs_add"
        elif theirs is None and ours is not None:
            kind = "delete_vs_update"
        elif theirs is not None and ours is None:
            kind = "update_vs_delete"
        else:
            kind = "update_vs_update"
        current = latest_row(connection, collection, key, head)
        return {"collection": collection, "key": key, "kind": kind,
                "base": before["document"] if before is not None else None,
                "main": ours["document"] if ours is not None else None,
                "branch": theirs["document"] if theirs is not None else None,
                "main_revision": current["rev"] if current is not None else 0,
                "resolutions": sorted(ACTIONS)}

    @staticmethod
    def _public_operation(item):
        return {"collection": item["collection"], "key": item["key"],
                "document": item["document"], "expires_at": item["expires_at"],
                "expected": item["expected"]}

    def _validate(self, connection, head, plan, actions, collections):
        """Apply schema and unique-index rules of the *current primary* catalog."""
        conflicts = []
        for _ in range(4):
            schema_issues = self._schema_conflicts(connection, plan)
            index_issues = self._index_conflicts(connection, head, plan, collections)
            issues = schema_issues + index_issues
            if not issues:
                return [], sorted(plan, key=lambda item: (item["document"] is not None,
                                                          item["collection"], item["key"]))
            changed = False
            retained = []
            for issue in issues:
                identity = (issue["collection"], issue["key"])
                action = actions.get(identity)
                if action is None or action["used"]:
                    retained.append(issue)
                    continue
                action["used"] = True
                if action["action"] == "take_main":
                    plan = [item for item in plan if (item["collection"], item["key"]) != identity]
                    changed = True
                elif action["action"] == "take_branch":
                    retained.append(issue)
                elif action["action"] == "delete":
                    replacement = next(item for item in plan if (item["collection"], item["key"]) == identity)
                    replacement["document"], replacement["expires_at"] = None, None
                    changed = True
                else:
                    replacement = next(item for item in plan if (item["collection"], item["key"]) == identity)
                    replacement["document"] = loads(dumps(action["document"]))
                    replacement["expires_at"] = None
                    changed = True
            if retained:
                return retained, plan
            if not changed:
                return issues, plan
        return self._schema_conflicts(connection, plan) + self._index_conflicts(connection, head, plan, collections), plan

    @staticmethod
    def _schema_conflicts(connection, plan):
        issues = []
        schemas = {}
        for item in plan:
            if item["document"] is None:
                continue
            if item["collection"] not in schemas:
                row = connection.execute("SELECT schema FROM collections WHERE name=?",
                                         (item["collection"],)).fetchone()
                schemas[item["collection"]] = loads(row[0]) if row is not None else None
            if schemas[item["collection"]] is None:
                issues.append({"collection": item["collection"], "key": item["key"], "kind": "unknown_collection",
                               "message": "collection no longer exists in the primary catalog",
                               "resolutions": ["take_main"]})
                continue
            try:
                validate_document(item["document"], schemas[item["collection"]])
            except InvalidDocument as exc:
                issues.append({"collection": item["collection"], "key": item["key"], "kind": "schema",
                               "message": "merged document does not satisfy the current primary schema: " + str(exc),
                               "resolutions": ["take_main", "take_document", "delete"]})
        return issues

    @staticmethod
    def _index_conflicts(connection, head, plan, collections):
        planned = {(item["collection"], item["key"]): item for item in plan}
        issues = []
        for definition in connection.execute(
                "SELECT * FROM index_defs WHERE unique_flag=1 ORDER BY collection,name").fetchall():
            if definition["collection"] not in collections:
                continue
            tokens = {}
            for row in visible_rows(connection, definition["collection"], head):
                identity = (definition["collection"], row["key"])
                if identity in planned:
                    continue
                token = scalar_token(field(loads(row["body"]), definition["field"]))
                if token is not None:
                    tokens[token] = row["key"]
            for item in sorted((entry for entry in plan if entry["collection"] == definition["collection"]),
                               key=lambda entry: entry["key"]):
                if item["document"] is None:
                    continue
                token = scalar_token(field(item["document"], definition["field"]))
                if token is None:
                    continue
                if token in tokens and tokens[token] != item["key"]:
                    issues.append({"collection": item["collection"], "key": item["key"], "kind": "index",
                                   "message": "merged document violates unique index " + definition["name"]
                                              + " held by key " + tokens[token],
                                   "resolutions": ["take_main", "take_document", "delete"]})
                tokens[token] = item["key"]
        return issues

    @staticmethod
    def _reject_unused(actions, conflicts=(), plan=()):
        """A resolution must target a reported conflict, a planned operation or a validation issue."""
        known = {(item["collection"], item["key"]) for item in conflicts}
        known |= {(item["collection"], item["key"]) for item in plan}
        for identity in sorted(actions):
            if not actions[identity]["used"] and identity not in known:
                raise Conflict("resolution does not match a conflict: " + identity[0] + "/" + identity[1])


def _resolve(action, theirs):
    selected = action["action"]
    if selected == "take_branch":
        return (theirs["document"] if theirs is not None else None,
                theirs["expires_at"] if theirs is not None else None)
    if selected == "delete":
        return None, None
    return action["document"], None
