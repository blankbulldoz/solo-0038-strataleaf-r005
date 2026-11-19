# 数据与兼容边界

物理表包括 meta、collections、versions、commits、receipts、index_defs、index_values、pins、consumers，以及分支表 branches、branch_catalog、branch_versions、branch_commits、branch_receipts、branch_merges。versions 的主键为集合、键、修订；body 的 SQL NULL 表示删除，JSON null 只能作为文档字段。collections 保存当前模式；index_values 是当前 head 的投影。commits 保存完整的操作序列；receipts 保存不受历史回收影响的请求摘要与结果。幂等键不能被不同操作复用，回放不重新执行前置条件。

分支不复制数据行：branch_catalog 记录分支自己的集合模式快照（创建时继承主库，之后可经 `refresh_catalog` 独立演进），branch_versions 只保存分支自己产生的键/修订，读取分支视图时以共同起点 base 的主库版本叠加分支覆盖。branch_commits 与 branch_receipts 是分支内提交日志及幂等回执，其修订号属于分支命名空间而非主库修订。branches.token 区分同名重建的分支实例；branches.synced 按集合记录最近一次成功合并对应的分支修订（clone 继承来源的映射并按克隆修订截断）；后续合并以该分支视图而非原始 base 作为三方祖先，未合并的集合仍以原始 base 比较，因此同分支可多次增量合并且部分集合合并不会掩盖其余集合的改动。branch_merges 以分支、token、分支修订、合并范围（选定集合）与处理方案记录已落地或已是最新的合并，重复提交同一份分支结果不会产生第二次修改。

合并落地的提交是普通 commits 行，操作带主库当前版本前置条件；其 payload 额外包含 merge 元数据（branch、base、branch_revision）。合并前按主库当前模式与唯一索引重算候选文档；分支中残留的不兼容文档必须显式 take_main、take_document 或 delete，不能默认写回。冲突、校验失败和回收后合并都在提交边界之前失败，不分配修订也不改写主库。

floor 是仍可读取的最早修订，不代表每条物理版本的最小修订；每个键在 floor 之前的最后版本可能是构造 floor 读视图的基准，必须保留。commits 仅需保存大于 floor 的提交，消费者从自己的 ack 之后读取。pin、消费者位置与活分支的 base 不能低于现存 floor。

模式包含 fields、required、additional，支持点路径类型；additional=false 限制顶层未知根字段，不递归禁止已声明对象中的子字段。字段不允许 null，除非该字段未声明类型；缺失可通过 required 控制。integer 排除 bool，number 接受整数及有限浮点。JSON 对象不允许非字符串键；浮点必须有限。索引忽略缺失字段与对象/数组，空值作为可索引标量，数值 1 和 1.0 共用索引值，true 与 1 不共用。

游标携带数据库身份、谓词摘要、读版本和最后键，并由数据库私有密钥签名。它不是权限凭据，不冻结模式，也不是保留租约。需要稳定的长时间读取，调用者自行管理具名保留点。

副本包是内部可信通道数据，校验和只用于检测传输损坏。正常备份恢复新主库保留逻辑内容而更换身份；副本初始化保留上游身份。分段追赶未完成时暂不提供当前索引，避免最新唯一约束错误约束中间历史视图。目录 epoch 可在没有文档提交时前进，最终空提交包仍能传递新的模式与索引。
