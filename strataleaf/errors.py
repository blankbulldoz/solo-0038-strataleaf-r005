class DatabaseError(Exception):
    """Base error for caller-visible database failures."""

class InvalidDocument(DatabaseError):
    pass

class Conflict(DatabaseError):
    pass

class MergeConflict(Conflict):
    """A branch merge needs explicit resolution; the main database was not changed."""

    def __init__(self, message, conflicts=None, plan=None):
        super().__init__(message)
        self.conflicts = list(conflicts or [])
        self.plan = plan or {}

class NotFound(DatabaseError):
    pass

class SnapshotTooOld(DatabaseError):
    pass

class InvalidCursor(DatabaseError):
    pass

class ReadOnly(DatabaseError):
    pass

class ReplicationError(DatabaseError):
    pass
