---
title: OLTP 联机事务处理数据库：ACID、事务隔离、MVCC 与行式存储引擎
series: 数据平台基础设施学习笔记
batch: 2
part: 4 / 5
---

# OLTP 联机事务处理（Online Transaction Processing）数据库学习笔记

> **关于本系列**
> 这是「数据平台基础设施」学习笔记（第二批）的第 **4** 篇。第二批围绕支撑现代数据平台的底座技术组织，建议按序阅读：
>
> 1. `06-data-lake.md` —— 数据湖（Data Lake）与对象存储上的开放表格式
> 2. `07-batch.md` —— 离线计算 / 批处理（Batch Processing）
> 3. `08-streaming.md` —— 实时计算 / 流处理（Stream Processing）
> 4. **`09-oltp.md` —— OLTP 联机事务处理数据库（本篇）**
> 5. `10-olap.md` —— OLAP 联机分析处理数据库（下一篇）
>
> 与第一批「数据建模学习笔记」（`01-inmon.md` ~ `05-dagster.md`）可交叉阅读：本篇讨论的 **3NF 规范化** 在 OLTP 中的作用，与 `01-inmon.md` 讲的 3NF 建模同源但目标不同；OLTP 与 OLAP 的分野，正是整个数仓 / Lakehouse 分层（见 `03-medallion.md`）之所以存在的物理起点。
>
> **目标读者**：有一定数据库 / 后端基础、希望系统理解事务型数据库内部机制的工程师。术语首次出现保留英文原文，正文关键论断附引用编号，文末「参考文献」给出联网检索得到的真实可访问 URL。
>
> **一句话定位**：OLTP 是「记录业务正在发生的事实」的数据库——它为大量并发的、短小的、以主键为中心的读写请求而生，靠 **ACID + 事务隔离 + 行式存储 + B+Tree 索引 + WAL** 这套组合，在高并发下既快又不出错。

---

## 0. 从一次转账说起

设想银行给你转账 100 元：从账户 A 扣 100，往账户 B 加 100。这两步必须「要么都成功、要么都不做」——如果扣了 A 却没加 B，钱就凭空消失了。与此同时，可能有成千上万的其他用户也在转账、查询余额、还房贷。数据库要保证：

- 每一笔转账都是**不可分割**的整体（原子性）；
- 转账前后账户总额守恒、不违反「余额不能为负」等业务约束（一致性）；
- 你的转账和别人的转账**并发**执行时，不会互相看到对方做了一半的中间状态（隔离性）；
- 一旦提示「转账成功」，哪怕机房立刻断电，这笔钱也不会丢（持久性）。

这四点就是 **ACID**，而专门为「支撑海量此类短事务」优化的数据库，就叫 **OLTP（Online Transaction Processing，联机事务处理）** 系统。Oracle 把 OLTP 定义为「由大量并发发生的事务组成的一类数据处理——网银、购物、订单录入、发短信都属于此类」[^oracle-oltp]。理解 OLTP，本质上就是理解「如何在高并发下既快又不出错地记录业务事实」。

---

## 1. 什么是 OLTP：负载画像

OLTP 不是某一个产品，而是一类**负载模式（workload pattern）**。它的典型特征可以画成一张画像[^clickhouse-oltp][^azure-oltp][^ibm-oltp]：

| 维度 | OLTP 的特征 |
|---|---|
| 面向对象 | 事务（transaction）——一组要么全做要么全不做的读写 |
| 单次操作规模 | 小。通常只碰几行到几十行，按主键或索引精确定位 |
| 读写比例 | 读写混合，写入频繁（INSERT / UPDATE / DELETE 是一等公民） |
| 并发度 | 极高。成千上万并发连接 / 会话同时操作 |
| 延迟要求 | 低。单请求毫秒级，用户在同步等待响应 |
| 吞吐指标 | TPS（每秒事务数）/ QPS（每秒查询数） |
| 数据时效 | 当前值（current-valued），反映「此刻」业务状态 |
| 数据规模 | 单表通常 GB ~ 数百 GB；热数据集中在近期 |
| 正确性要求 | 极高，必须 ACID；错一分钱都不行 |

一句话概括业界的共识：**OLTP 为「大量小操作、快速完成」优化**（many small operations completed quickly），而它的对立面 OLAP 恰好相反——为「少量大操作、吞吐优先」优化[^clickhouse-unify]。

### 1.1 什么样的操作是「OLTP 操作」

- 「把订单 #12345 的状态从『待支付』改为『已支付』」——按主键更新一行。
- 「查询用户 U1001 的账户余额」——按主键点查（point lookup）一行。
- 「新增一条评论」——插入一行。
- 「下单：扣库存 + 建订单 + 记流水」——一个跨多表、多行的事务。

它们的共同点：**知道要碰哪几行**（有明确的键），**碰的行很少**，**要立刻返回**，并且**经常要改数据**。这与「扫描过去三年所有订单、按地区聚合月度 GMV」这种分析型查询（OLAP）在物理上是两种完全不同的活儿——这一点是本篇与下一篇 `10-olap.md` 的主线，第 8 节详述。

---

## 2. ACID：OLTP 的正确性契约

ACID 是四个单词的首字母缩写，指把「一串数据库写入」变成一个可靠「事务」的四项保证：**Atomicity（原子性）、Consistency（一致性）、Isolation（隔离性）、Durability（持久性）**[^stanford-acid]。它们是 OLTP 系统对应用开发者做出的核心承诺。

### 2.1 Atomicity 原子性——全有或全无

一个事务内的所有操作被视为**单一、不可分割**的单元：要么全部成功提交（COMMIT），要么在任何一步失败时全部回滚（ROLLBACK），绝不会停在「做了一半」的中间态[^stanford-acid][^azure-oltp]。

回到转账例子：

```sql
BEGIN;
  UPDATE accounts SET balance = balance - 100 WHERE id = 'A';  -- 扣款
  UPDATE accounts SET balance = balance + 100 WHERE id = 'B';  -- 入账
COMMIT;  -- 两条一起生效；若中途崩溃则两条都不生效
```

若在两条 UPDATE 之间进程崩溃，原子性保证「扣了 A 没加 B」这种半成品状态不会留在数据库里。实现原子性的关键机制是 **undo log（回滚日志）**：修改前先记录旧值，回滚时据此撤销。

> ⚠️ 术语澄清：Kleppmann 在《Designing Data-Intensive Applications》中特别指出，ACID 里的 Atomicity 与并发无关，指的是「可中止性（abortability）」——即出错时能干净地丢弃整个事务；它和多线程语境里「原子操作」的含义不是一回事[^ddia-ch7]。

### 2.2 Consistency 一致性——不破坏不变量

事务必须让数据库**从一个合法状态转移到另一个合法状态**，不违反任何已声明的完整性约束：主键唯一、外键引用、CHECK 约束（如 `balance >= 0`）、唯一索引、触发器等[^azure-oltp][^stanford-acid]。

值得强调的是，ACID 里的 C 与 CAP 定理里的 C 不是同一个概念。Kleppmann 甚至认为一致性某种程度上是「应用层的属性」——是应用定义了什么叫「合法」，数据库只负责在事务边界处强制检查这些约束[^ddia-ch7]。数据库能帮你的，是提供**声明式约束**这把武器。

### 2.3 Isolation 隔离性——并发如同串行

并发执行的多个事务之间要相互隔离：一个事务不应看到另一个未提交事务的中间结果。**最强的隔离——可串行化（Serializable）——保证并发事务的执行结果，等价于它们以某种顺序一个接一个串行执行的结果**[^pg-txniso][^berenson-critique]。

隔离性是四个特性里最微妙、代价最高、也最常被「打折」的一个——因为完全的串行化会严重牺牲并发性能。于是数据库提供了**多档隔离级别（isolation levels）**让你在「正确性」与「性能」之间权衡，这是第 3 节的主题。

### 2.4 Durability 持久性——提交即不丢

一旦事务提交成功，它的修改就是**永久的**，即使随后立刻发生断电、进程崩溃、操作系统宕机，重启后数据依然在[^azure-oltp][^stanford-acid]。

持久性的经典实现是 **WAL（Write-Ahead Logging，预写日志）**：提交时并不急着把改动的数据页刷回磁盘，而是先把「我改了什么」的日志顺序写入磁盘；崩溃重启后，重放（replay）日志即可恢复所有已提交但尚未落盘的改动。MySQL InnoDB 的 **redo log** 正是这一原理的实现——它明确说明「未在崩溃前写完数据文件的修改，会在初始化阶段、接受连接之前自动重放」[^mysql-redo]。第 5 节详解。

### 2.5 一张图看懂事务生命周期

```
   BEGIN
     │
     ▼
 ┌─────────────┐   写前先记 undo（为回滚） + redo（为持久化）
 │  执行 SQL    │──────────────────────────────────────────┐
 │  读 / 写多行 │                                           │
 └─────────────┘                                           ▼
     │                                              ┌──────────────┐
     │  出错 / 显式 ROLLBACK / 崩溃                  │ WAL(redo log) │
     ▼                                              │  顺序落盘      │
 ┌─────────┐                                        └──────────────┘
 │ ROLLBACK│  用 undo log 撤销全部改动（原子性）           │
 └─────────┘                                              │ COMMIT
     │                                                    ▼
     │                                          「成功」返回给客户端
     ▼                                          （此刻断电也不丢——持久性）
   事务结束                                       数据页稍后惰性刷盘
```

---

## 3. 事务隔离级别、并发异常与 MVCC

隔离性之所以复杂，是因为「完全隔离（串行化）」太贵。数据库因此定义了几档**隔离级别**，级别越低并发越好、但可能出现越多**并发异常（anomalies / phenomena）**。ANSI SQL-92 用三种「现象」来定义隔离级别：**脏读（Dirty Read）、不可重复读（Non-repeatable Read）、幻读（Phantom）**[^berenson-critique]。

### 3.1 三类经典并发异常

**脏读（Dirty Read）**：事务 T1 读到了事务 T2 **尚未提交**的修改；若 T2 随后回滚，T1 就读到了一个「从未真正存在过」的值。

```
T2: UPDATE accounts SET balance=0 WHERE id='A';  -- 未提交
T1:   SELECT balance FROM accounts WHERE id='A'; -- 读到 0（脏数据）
T2: ROLLBACK;                                    -- 那个 0 从未存在过
```

**不可重复读（Non-repeatable Read）**：同一事务内两次读**同一行**，结果不同——因为期间另一个事务提交了对该行的修改。针对的是**已存在行被改 / 被删**。

```
T1: SELECT balance FROM accounts WHERE id='A';  -- 读到 500
T2:   UPDATE accounts SET balance=600 WHERE id='A'; COMMIT;
T1: SELECT balance FROM accounts WHERE id='A';  -- 又读到 600，前后不一致
```

**幻读（Phantom Read）**：同一事务内两次执行**同一范围查询**，返回的**行集合**变了——因为期间另一个事务插入 / 删除了满足条件的行。针对的是**符合条件的行数变化**（「多出 / 少了幻影行」）。

```
T1: SELECT count(*) FROM orders WHERE amount > 1000;  -- 5 行
T2:   INSERT INTO orders(amount) VALUES (2000); COMMIT;
T1: SELECT count(*) FROM orders WHERE amount > 1000;  -- 6 行，多出一个「幻影」
```

> 不可重复读 vs 幻读的区别常被搞混：**不可重复读关注「同一行的值变了」，幻读关注「满足条件的行数变了」**。前者靠行锁 / 行版本即可防，后者需要范围锁 / 谓词锁（predicate locking）或快照来防。

### 3.2 四档标准隔离级别

SQL 标准定义了四档隔离级别，按「允许哪些异常」划分[^pg-txniso][^berenson-critique]：

| 隔离级别 | 脏读 | 不可重复读 | 幻读 | 序列化异常 |
|---|:---:|:---:|:---:|:---:|
| Read Uncommitted（读未提交） | 可能 | 可能 | 可能 | 可能 |
| Read Committed（读已提交） | 不会 | 可能 | 可能 | 可能 |
| Repeatable Read（可重复读） | 不会 | 不会 | 可能¹ | 可能 |
| Serializable（可串行化） | 不会 | 不会 | 不会 | 不会 |

关键在于：**标准只规定了每档「至少」要防住哪些异常，没规定「必须只用」某种实现，也没禁止实现得比标准更强**。这导致同名隔离级别在不同数据库里行为并不完全一致，是 OLTP 里最大的坑之一。PostgreSQL 官方文档就给出了它自己的真实行为矩阵，与标准有两处显著差异[^pg-txniso]：

- **PostgreSQL 没有真正的 Read Uncommitted**：请求它会得到 Read Committed 的行为，因为在其 MVCC 架构下脏读根本不可能发生——任何隔离级别都读不到未提交数据[^pg-txniso]。
- **PostgreSQL 的 Repeatable Read 会连幻读一起防住**（注¹）：因为它用**快照隔离（Snapshot Isolation）**实现，比标准要求的更强[^pg-txniso]。

这正是 1995 年经典论文《A Critique of ANSI SQL Isolation Levels》（Berenson、Bernstein、Gray 等）批判的核心：ANSI 用英文口语定义的三种现象**含糊且不完备**，无法准确刻画包括快照隔离在内的多种流行实现[^berenson-critique]。这篇论文引入了 **Snapshot Isolation** 的严格定义，是理解现代 MVCC 数据库隔离语义的必读文献。

### 3.3 MVCC：读不阻塞写，写不阻塞读

现代主流 OLTP 数据库（PostgreSQL、MySQL/InnoDB、Oracle）实现隔离性的核心武器不是「加锁」，而是 **MVCC（Multiversion Concurrency Control，多版本并发控制）**。

PostgreSQL 官方文档一语道破 MVCC 的本质优势：**「查询（读）时获取的锁与写入时获取的锁互不冲突，所以读永远不会阻塞写、写永远不会阻塞读」**[^pg-mvcc-intro]。传统的锁模型里读写会互相排斥；MVCC 则让「每条 SQL 语句看到的是数据在某一时刻的快照（snapshot）」[^pg-mvcc-intro]。

**MVCC 的核心思想**：写入时**不覆盖旧数据**，而是**创建新版本**；每个数据行版本（在 PG 中叫 tuple）携带「由哪个事务创建 / 由哪个事务删除」的元信息（在 PG 中即 `xmin` / `xmax`）。读取时，事务根据自己的快照和**可见性规则（visibility rules）**决定「我该看到这一行的哪个版本」[^pg-mvcc-deep][^boringsql-mvcc]。

```
一行数据的多个版本（以 PostgreSQL 为例）：

  row_id=A  ┌───────────────┬───────────────┬───────────────┐
            │ v1 balance=500│ v2 balance=600│ v3 balance=400│
            │ xmin=100      │ xmin=205      │ xmin=310      │
            │ xmax=205      │ xmax=310      │ xmax=null     │
            └───────────────┴───────────────┴───────────────┘
                    ▲               ▲               ▲
  快照 A（事务 150 开始）能看到 v1（v1 在 100 提交、205 尚未发生）
  快照 B（事务 250 开始）能看到 v2
  快照 C（当前，事务 350）  能看到 v3
```

- **Read Committed** 下，每条语句开始时取一个**新快照**——所以同一事务内两条 SELECT 可能看到不同已提交数据（会不可重复读）[^pg-txniso]。
- **Repeatable Read（快照隔离）** 下，整个事务**共用第一条语句时取的那一个快照**——全程看到一致的数据，因此连幻读都不会发生。代价是：若两个事务改同一行，后提交者会收到 `ERROR: could not serialize access due to concurrent update`，应用需**重试**[^pg-txniso]。

**MVCC 的代价**：旧版本不能立刻删（可能还有事务在用），会累积成「膨胀（bloat）」，PostgreSQL 需要 **VACUUM** 后台清理死元组，InnoDB 则用 **purge** 线程清理 undo 版本[^pg-arch][^jusdb-innodb]。这是 MVCC「读写不互锁」的代价——空间换并发。

### 3.4 Serializable 的两种实现路线

最强的 Serializable 有两条实现路线：

1. **严格两阶段锁（Strict 2PL）+ 范围锁 / 谓词锁**：靠加锁把并发强行退化为串行等价。MySQL InnoDB 的 Serializable 走这条路，读也要加共享锁。
2. **可串行化快照隔离（SSI，Serializable Snapshot Isolation）**：PostgreSQL 9.1+ 采用。它在快照隔离基础上，用**谓词锁（`SIReadLock`，不阻塞、不死锁）**监控事务间的读写依赖；一旦发现可能产生「无法对应任何串行顺序」的危险依赖环，就回滚其中一个事务，报 SQLSTATE `40001`[^pg-txniso]。

PostgreSQL 文档给出的经典例子：两个事务分别按 `class=1` 和 `class=2` 求和再交叉插入，在 Repeatable Read 下都能提交、但结果不对应任何串行顺序，Serializable(SSI) 会检测到并回滚其一[^pg-txniso]。**因此任何用 Serializable 的应用都必须实现通用重试机制**（捕获 `40001` 后重跑事务）。

---

## 4. 存储引擎与索引：为什么行式 + B+Tree

隔离级别决定「并发时看到什么」，而**存储引擎**决定「数据在磁盘上怎么摆、怎么找」。OLTP 的负载画像（点查、小范围扫描、频繁单行写）直接决定了它的存储层选择：**行式存储 + B+Tree 索引**。

### 4.1 行式存储（row-oriented storage）

行式存储把**一整行的所有列连续存放**在磁盘上；列式存储则把**同一列的所有值连续存放**[^clickhouse-rowvscol]。

```
行式存储（OLTP，如 PostgreSQL / InnoDB）：
  磁盘块1: [id=1, name=Alice, age=30, city=BJ] [id=2, name=Bob, age=25, city=SH] ...
           └────────── 一行连续 ──────────┘

列式存储（OLAP，如 ClickHouse，见 10-olap.md）：
  块A(id):   [1, 2, 3, 4, ...]
  块B(name): [Alice, Bob, ...]
  块C(age):  [30, 25, ...]
```

为什么 OLTP 选行式？因为 OLTP 的典型操作是「**取出 / 修改某一行的全部或大部分列**」（读一个用户的完整信息、更新一个订单）。行式存储下，一行数据在物理上相邻，一次 I/O 就能把整行读出或写入；而列式存储要拼出一整行，得从多个分散的列块里各取一个值，代价高。反过来，「只取一列、扫描百万行做聚合」这种 OLAP 操作，列式才占优[^clickhouse-rowvscol][^jusdb-olapoltp]。ClickHouse 官方一句话总结：**「行存（如 Postgres、MySQL）赢在点查和单行写；列存赢在大规模聚合扫描」**[^clickhouse-rowvscol]。

### 4.2 B+Tree 索引：OLTP 索引的默认选择

绝大多数 OLTP 索引是 **B+Tree**。MySQL 官方明确说明：「大多数 MySQL 索引（PRIMARY KEY、UNIQUE、INDEX、FULLTEXT）都以 B-tree 存储」[^mysql-indexes]。B+Tree 之所以主导 OLTP，是因为它同时擅长 OLTP 需要的两件事：**按键精确点查（=）** 和 **小范围区间扫描（>、<、BETWEEN、IN）**[^mysql-colindex]。

B+Tree 的结构要点（与普通 B-tree 的关键区别）：**只有叶子节点存实际数据 / 行指针，内部节点只存索引键用于导航，且叶子节点之间用链表串联**——这让区间扫描可以顺着叶子链表顺序读，无需回溯上层。

```
                    ┌─────────────────┐
       内部节点      │   [30]   [60]   │   ← 只存导航键
                    └────┬─────┬────┬──┘
              ┌──────────┘     │    └──────────┐
              ▼                ▼               ▼
        ┌───────────┐   ┌───────────┐   ┌───────────┐
 叶子层  │10│20│ →     │30│40│50│ →   │60│70│80│      ← 存数据/行指针
        └───────────┘   └───────────┘   └───────────┘
         叶子之间双向链表相连 →→→（区间扫描顺链表走，O(1) 跨页）

  查 id=40：根据内部节点 [30][60] 导航 → 落到中间叶子 → 命中 40
  查 id BETWEEN 40 AND 70：定位到 40 后，顺叶子链表扫到 70 即可
  树高通常 3~4 层即可索引上亿行 → 点查只需 3~4 次页 I/O
```

InnoDB 给每个页分配「层级（level）」：叶子页为 level 0，向上递增；一棵能索引上亿行的 B+Tree 通常只有 3~4 层，意味着一次点查只要 3~4 次页访问[^mysql-innodb-index]。

### 4.3 聚簇索引（clustered index）vs 二级索引

InnoDB 有一个对 OLTP 性能影响巨大的设计：**表数据本身就按主键组织成一棵 B+Tree，这棵树叫聚簇索引（clustered index）**。MySQL 文档原话：「代表整张表的那棵 B-tree 索引就是聚簇索引，它按主键列组织；聚簇索引的叶子节点里直接存着行数据」[^mysql-rowformat]。

由此带来两个 OLTP 工程要点：

- **按主键查最快**：因为主键 B+Tree 的叶子节点直接就是整行数据，一次树遍历就拿到全部列，无需二次查找。
- **二级索引（secondary index）需要「回表」**：二级索引的叶子存的是「索引列值 + 主键值」，按二级索引查到主键后，还要**再走一次聚簇索引**才能取到完整行——这就是所谓的「回表」。这也解释了为什么 InnoDB 里主键不宜过长（每个二级索引都要冗余存主键），以及为什么覆盖索引（covering index）能显著提速。

PostgreSQL 则不同：它的表是**堆表（heap）**，主键也是一个独立的 B+Tree 索引，索引叶子存的是指向堆中物理位置的 `ctid`——没有 InnoDB 那种「表即主键树」的聚簇结构。这是两大 OLTP 引擎最重要的架构差异之一。

### 4.4 WAL / redo log：持久性与崩溃恢复的引擎

第 2.4 节说过持久性靠 WAL。这里补充其运作机理。**WAL 的核心原则：数据页真正落盘之前，先把「打算怎么改」的日志顺序写入磁盘**[^mysql-redo]。

以 InnoDB redo log 为例[^mysql-redo]：

- 它是「应用到数据页内容上的变更的预写日志，为所有变更提供持久性」；
- 数据通过一个**只增的 LSN（Log Sequence Number）** 跟踪；
- 提交时，只需保证 redo log 安全落盘即可返回「成功」——**被修改的数据页可以延后、惰性地刷回表空间**；
- 崩溃重启时，InnoDB 从最近一个 **checkpoint** 的 LSN 开始扫描 redo log，重放尚未落盘的已提交改动，恢复到崩溃前的一致状态。

为什么这套机制对 OLTP 至关重要？因为它把「随机写数据页」变成了「**顺序写日志**」——顺序 I/O 远快于随机 I/O，这让高频提交（高 TPS）成为可能，同时又不牺牲持久性。这是 OLTP 引擎能兼顾「快」与「不丢」的核心工程技巧。

> 三种日志别搞混：**redo log（重做，为持久性 / 崩溃恢复，记「新值」）**、**undo log（回滚 + MVCC 旧版本，记「旧值」）**、**binlog（MySQL server 层的逻辑日志，用于主从复制与时间点恢复）**。InnoDB 的 undo log 同时服务于原子性回滚和 MVCC 多版本读。

---

## 5. 典型 OLTP 数据库

主流 OLTP 数据库在 ACID / MVCC / 行存 / B+Tree / WAL 这套骨架上一致，差异在实现细节与生态定位。

| 数据库 | 存储 / 并发核心 | OLTP 特点 |
|---|---|---|
| **PostgreSQL** | 堆表 + B+Tree 索引；MVCC（tuple 版本 + `xmin/xmax`）；WAL；SSI 实现真正 Serializable | 功能最全的开源数据库；标准兼容性强；隔离语义严谨（Repeatable Read = 快照隔离）；需 VACUUM 清膨胀[^pg-mvcc-intro][^pg-txniso][^pg-arch] |
| **MySQL / InnoDB** | 聚簇索引（表即主键 B+Tree）；MVCC（undo 版本）；redo log(WAL) + doublewrite buffer；binlog 复制 | Web 领域最流行；聚簇索引使主键点查极快；生态与运维成熟[^mysql-rowformat][^mysql-redo][^jusdb-innodb] |
| **Oracle Database** | 多版本读一致性；redo / undo；行存 | 传统企业级 OLTP 标杆；Oracle 本身把 OLTP 定义为「大量并发短事务」[^oracle-oltp] |
| **SQL Server** | B+Tree（聚簇 / 非聚簇）；默认锁式并发，可选 RCSI 走行版本 MVCC | 微软生态企业级 OLTP |
| **分布式 NewSQL**（CockroachDB、TiDB、Spanner、YugabyteDB） | 分片 + Raft/Paxos 复制 + 分布式事务 | 在保留 SQL 与 ACID 的前提下做水平扩展，解决单机 OLTP 容量 / 可用性上限 |

选型的第一性原则：**只要是「记录业务事实、要求强一致、以点查 / 短事务为主」的场景，就选行式 OLTP 数据库**；国内 Web 项目 MySQL/InnoDB 是事实标准，需要复杂查询 / 强标准兼容 / GIS / JSON 等能力时 PostgreSQL 往往是更好的选择。

---

## 6. 规范化建模（3NF）在 OLTP 中的作用

OLTP 库的表结构通常做**规范化（normalization）**，目标一般到 **第三范式（3NF）**。这与 `01-inmon.md` 里讲的 Inmon EDW 用 3NF 建模同源，但**动机侧重点不同**——那里是为「企业级集成 / 单一可信源」，这里是为「事务写入的正确与高效」。

### 6.1 3NF 一句话回顾

- **1NF**：每个字段原子、不可再分，无重复组。
- **2NF**：在 1NF 基础上，非主键字段完全依赖于整个主键（消除部分依赖）。
- **3NF**：在 2NF 基础上，非主键字段之间不存在传递依赖——**「每个非键字段都只依赖于键、依赖于整个键、且只依赖于键」**。

核心思想（见 `01-inmon.md` 第 5 节）：**每一条事实只在一个地方存储一次**，消除数据冗余。

### 6.2 为什么 OLTP 偏爱规范化：防更新异常

规范化对 OLTP 的价值，集中在**避免三种更新异常（update anomalies）**——这恰恰是「写频繁」的 OLTP 最怕的：

- **更新异常（update anomaly）**：若客户地址在 1000 行订单里冗余存了 1000 份，客户搬家就要改 1000 行；漏改任何一行即产生不一致。规范化后地址只存一份，改一行即可。
- **插入异常（insertion anomaly）**：想新增一个「还没有任何订单的客户」，若客户信息只依附在订单表里就无处安放。
- **删除异常（deletion anomaly）**：删掉某客户最后一张订单，可能连带丢失该客户本身的信息。

**规范化把「一处事实一处存」这件事，转化为写入时的天然一致性保障**——配合外键约束，数据库能从结构上帮 OLTP 应用堵住大量不一致隐患。Kleppmann 也指出，规范化数据的思路是「用 ID 引用而非重复人类可读信息」，其好处正是一致性与写入效率[^ddia-ch3]。

### 6.3 与 OLAP 建模的对照（承上启下）

代价是：3NF 把数据拆进很多张窄表，**查询时要大量 JOIN**。对 OLTP 无所谓（一次只碰几行、JOIN 面窄）；但对「扫描海量历史、多表聚合」的 OLAP 却是灾难。所以 OLAP 反其道而行——**反规范化（denormalization）** 成宽表 / 星型模型（见 `02-kimball.md` 维度建模、`10-olap.md`）。

> **一句话对照**：OLTP 用 3NF **为写入正确性优化**；OLAP 用星型 / 宽表 **为读取 / 聚合性能优化**。同一份业务数据，在两种系统里长成完全不同的形状——这正是数仓分层（`03-medallion.md`）存在的根本原因。

---

## 7. OLTP vs OLAP：本质区别

OLTP（联机事务处理）与 OLAP（Online Analytical Processing，联机分析处理）是每个数据库都要面对的**两类根本负载**[^clickhouse-oltp]。理解它们的分野，是理解整个现代数据栈（数据湖 / 数仓 / Lakehouse）为什么要分层的起点。

| 维度 | OLTP（本篇） | OLAP（下一篇 `10-olap.md`） |
|---|---|---|
| 核心用途 | 记录业务正在发生的事实、支撑日常运营 | 从历史数据中分析、聚合、发现洞察 |
| 负载模式 | 大量小操作、快速完成 | 少量大操作、吞吐优先 |
| 典型查询 | 按主键点查 / 更新几行；短事务 | 扫描百万~十亿行、多维聚合（SUM/GROUP BY） |
| 读写特征 | 读写混合，写频繁（INSERT/UPDATE/DELETE） | 批量写入 + 海量只读扫描，几乎不改单行 |
| 存储方式 | **行式存储**（整行连续） | **列式存储**（整列连续，高压缩） |
| 索引 | B+Tree、聚簇 / 二级索引 | 列存 + 分区 + 稀疏索引 / zone map |
| 建模 | **3NF 规范化**（防更新异常） | 星型 / 雪花 / 宽表，**反规范化**（省 JOIN） |
| 数据时效 | 当前值，毫秒级新鲜 | 历史累积，可容忍一定延迟 |
| 并发度 | 极高（成千上万并发短事务） | 相对低（少量重查询） |
| 性能指标 | TPS / QPS、单请求延迟 | 查询吞吐、扫描 GB/s |
| 典型产品 | PostgreSQL、MySQL、Oracle、SQL Server | ClickHouse、Snowflake、BigQuery、DuckDB |

一句话：**OLTP 优化「快速完成大量小操作」，OLAP 优化「高吞吐处理少量大操作」——两者性能画像根本相反**[^clickhouse-unify]。

### 7.1 为什么 OLTP 不适合分析型查询

把分析查询直接跑在生产 OLTP 库上，是很多团队踩过的坑。原因是物理层面的：

1. **行式存储读多列吃亏**：分析查询常常「只要几列、但要扫全表」。行存必须把每一行的**所有列**都读进内存才能取出你要的那几列，白白浪费大量 I/O 与内存带宽；列存只读需要的列块[^clickhouse-rowvscol]。
2. **无压缩优势**：列存中同列同类型数据相邻，压缩率极高（常 10 倍以上）；行存混合类型，压缩效果差，扫描数据量大。
3. **索引帮不上大扫描**：B+Tree 为点查而生；一旦查询要碰全表的大部分行，走索引反而比全表扫描更慢（随机 I/O），优化器会放弃索引。
4. **抢占生产资源、破坏 SLA**：一条扫全表的分析查询会长时间占用 CPU / 内存 / I/O 与缓冲池，挤占正常事务，拉高线上延迟；在 MVCC 库里长事务还会**阻塞 VACUUM / purge**，加剧膨胀。
5. **锁与快照压力**：长时间运行的分析事务会持有旧快照，让数据库无法回收旧行版本。

**结论：让 OLTP 专心做事务，把分析负载搬到 OLAP 系统**——通过 ETL / ELT / CDC 把数据从 OLTP 同步到数仓 / Lakehouse。这正是 `03-medallion.md`、`04-dbt.md` 整条数据管线存在的意义：Bronze 层往往就是 OLTP 库的镜像。

### 7.2 HTAP：想打破这堵墙

既然要在两套系统间搬数据（还带延迟），能不能一套系统同时干好两件事？这就是 **HTAP（Hybrid Transactional/Analytical Processing，混合事务分析处理）**——这个术语由 **Gartner 于 2014 年提出**，用来描述「在同一份数据、甚至同一个事务里同时支持 OLTP 与 OLAP」的系统架构，其口号是「打破事务处理与分析之间的那堵墙（breaking the wall）」，以实现「基于最新业务数据的实时分析」[^htap-wiki][^htap-survey]。

常见 HTAP 实现思路：

- **双引擎 + 内部同步**：一份数据同时维护行存（供 OLTP）与列存（供 OLAP）两种副本，内部自动同步。TiDB（TiFlash 列存副本）、SQL Server（列存索引）是代表。
- **内存 + 混合存储**：如 SAP HANA、HyPer 等把两类负载放进同一内存引擎。

HTAP 的现实权衡：它降低了数据新鲜度损耗和架构复杂度，但要在同一系统里同时优化两种相反的物理特征，往往在极端 OLTP 或极端 OLAP 场景下都不如专用系统。学术界 2024 年的综述指出，自 Gartner 造词以来已涌现大量 HTAP 数据库，核心挑战正是**在保证事务性能的同时提供实时分析**[^htap-arxiv]。对多数团队，「OLTP + OLAP 两套系统 + 数据管线」仍是主流；HTAP 是「实时性要求极高、又不想维护复杂管线」时的备选。

---

## 8. 适用场景、优点与局限

### 8.1 适用场景

- 交易系统：银行转账、支付、证券下单、电商订单。
- 业务应用后端：用户 / 账户 / 库存 / 订单 / 权限等 CRUD。
- 任何「要求强一致、以主键点查 / 短事务为主、数据要毫秒级新鲜」的系统。

在本项目 `tushare-dashboard` 语境下：应用侧的缓存与元数据（如各接口的 SQLite 缓存、任务状态）本质是轻量 OLTP 用法——高频点查、单行写、要求一致；而 Tushare 行情数据的**批量分析**（跨股票、跨时间聚合）则属于 OLAP，走的是 Lakehouse / DuckDB 那条线。这个「OLTP 存状态、OLAP 做分析」的分工，正是本系列反复强调的分层原则的微观体现。

### 8.2 优点

- **强一致 + ACID**：数据正确性有硬保证，适合金钱 / 库存等不容出错的场景。
- **高并发低延迟**：为大量并发短事务优化，单请求毫秒级。
- **成熟稳定**：几十年工程积累，工具 / 运维 / 人才生态完善。
- **规范化 + 约束**：从结构上防止数据异常。

### 8.3 局限

- **不擅长大规模分析**：行存 + B+Tree 面对全表扫描聚合力不从心（第 7.1 节）。
- **单机容量 / 吞吐上限**：垂直扩展有天花板；突破需分库分表或上 NewSQL，复杂度陡增。
- **MVCC 膨胀**：高写入 + 长事务会累积死版本，需持续 VACUUM / purge 运维。
- **JOIN 密集**：3NF 建模下复杂查询 JOIN 多，跨大表 JOIN 性能差。

---

## 9. 小结与承上启下

- **OLTP 是一类负载**：大量并发的、短小的、以主键为中心的读写事务，要求毫秒延迟与强一致。
- **ACID 是正确性契约**：原子性（undo 回滚，全有或全无）、一致性（约束不被破坏）、隔离性（并发如同串行）、持久性（WAL/redo，提交即不丢）。
- **隔离级别是权衡旋钮**：读未提交 / 读已提交 / 可重复读 / 可串行化，防住脏读 / 不可重复读 / 幻读 / 序列化异常的程度递增；同名级别在不同库行为不同（PG 无真 Read Uncommitted、其 Repeatable Read = 快照隔离），坑多，务必查各自文档。
- **MVCC 是并发引擎**：多版本 + 快照 + 可见性规则，让读写互不阻塞，代价是膨胀与清理。
- **行存 + B+Tree + WAL 是存储骨架**：行存利于整行读写，B+Tree 兼顾点查与区间，WAL 把随机写变顺序写、兼顾快与不丢。
- **3NF 为写入正确性服务**：一处事实一处存，防更新 / 插入 / 删除异常；与 OLAP 的反规范化正好相反。
- **OLTP ≠ OLAP**：负载、存储、建模、查询特征全线相反；别拿 OLTP 库跑分析。HTAP 想打破这堵墙，但两套系统 + 数据管线仍是主流。

**下一篇 `10-olap.md`** 将接着这条线，深入 OLAP：列式存储的物理原理与压缩、向量化 / MPP 执行、星型 / 宽表建模、以及 ClickHouse / DuckDB / Snowflake 等分析引擎——看数据从 OLTP 流出后，如何在分析侧被重新塑形以榨取吞吐。至此，「事务侧（本篇）」与「分析侧（下篇）」合起来，构成整个数据平台最底层的两块基石。

---

## 参考文献

> 以下链接均来自本文撰写时（2026-07）的联网检索，为真实可访问来源。ACID / 隔离级别以 PostgreSQL 官方文档与 Berenson 等 1995 SIGMOD 论文为权威依据；存储引擎细节以 MySQL / PostgreSQL 官方文档为准；概念性论断辅以 Kleppmann《Designing Data-Intensive Applications》与权威技术资料。

[^oracle-oltp]: Oracle — What Is OLTP?（OLTP 定义：大量并发短事务，网银 / 购物 / 订单 / 短信）. <https://www.oracle.com/database/what-is-oltp/>
[^azure-oltp]: Microsoft Azure Architecture Center — Online Transaction Processing (OLTP)（OLTP 特征与 ACID 的非正式与正式定义）. <https://learn.microsoft.com/en-us/azure/architecture/data-guide/relational-data/online-transaction-processing>
[^ibm-oltp]: IBM — What Is Online Transactional Processing (OLTP)?（多用户并发访问同一数据、并发控制保证完整性）. <https://www.ibm.com/think/topics/oltp>
[^stanford-acid]: Stanford CS145 — ACID Properties（ACID 四项保证：原子性/一致性/隔离性/持久性的教学定义）. <https://cs145.stanford.edu/Module4-Transactions/acid-properties.html>
[^clickhouse-oltp]: ClickHouse — OLTP vs OLAP（两类负载定义：OLTP 优化高频单行读写、OLAP 优化聚合扫描）. <https://clickhouse.com/resources/engineering/oltp-vs-olap>
[^clickhouse-unify]: ClickHouse — HTAP databases, zero-ETL, and best-of-breed architectures（OLTP 优化「大量小操作快速完成」，OLAP 相反）. <https://clickhouse.com/resources/engineering/unifying-oltp-and-olap>
[^clickhouse-rowvscol]: ClickHouse — Row-oriented vs column-oriented databases（行存赢点查/单行写，列存赢聚合扫描；存储布局差异）. <https://clickhouse.com/resources/engineering/row-vs-column-database>
[^jusdb-olapoltp]: JusDB — OLAP vs OLTP: Database Workload Selection（OLTP 用行存优化点读写，OLAP 用列存优化扫描聚合）. <https://www.jusdb.com/blog/olap-vs-oltp-database-workload-selection>
[^berenson-critique]: Berenson, Bernstein, Gray, Melton, O'Neil, O'Neil — A Critique of ANSI SQL Isolation Levels（SIGMOD 1995；ANSI 三现象定义含糊不完备，引入 Snapshot Isolation 严格定义）. <https://arxiv.org/abs/cs/0701157> ；原始 PDF：<https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/tr-95-51.pdf>
[^pg-txniso]: PostgreSQL 官方文档 — Transaction Isolation（四档隔离级别真实行为矩阵；PG 无真 Read Uncommitted；Repeatable Read = 快照隔离；Serializable = SSI + 谓词锁）. <https://www.postgresql.org/docs/current/transaction-iso.html>
[^pg-mvcc-intro]: PostgreSQL 官方文档 — Concurrency Control / MVCC Introduction（MVCC 本质：读锁与写锁不冲突，读不阻塞写、写不阻塞读；每条语句看到快照）. <https://www.postgresql.org/docs/current/mvcc-intro.html>
[^pg-mvcc-deep]: TheLinuxCode — MVCC in PostgreSQL: A Practical Deep Dive（可见性规则、写入创建新版本、读按快照选版本）. <https://thelinuxcode.com/multiversion-concurrency-control-mvcc-in-postgresql-a-practical-deep-dive-for-high-concurrency-systems/>
[^boringsql-mvcc]: boringSQL — PostgreSQL MVCC, Byte by Byte（每个 tuple 携带 xmin/xmax 双字段，无「当前版本」表）. <https://boringsql.com/posts/postgresql-mvcc-byte-by-byte/>
[^pg-arch]: JusDB — PostgreSQL Architecture Deep Dive: Process Model, MVCC, WAL（MVCC 给读者无阻塞快照，代价是膨胀需 VACUUM 回收）. <https://www.jusdb.com/blog/postgresql-architecture-deep-dive>
[^mysql-redo]: MySQL 8.0 Reference Manual — InnoDB Redo Log（redo 是变更的预写日志，提供持久性；LSN 跟踪；崩溃后从 checkpoint LSN 重放）. <https://dev.mysql.com/doc/refman/8.0/en/innodb-redo-log.html>
[^mysql-indexes]: MySQL Reference Manual — How MySQL Uses Indexes（PRIMARY KEY/UNIQUE/INDEX/FULLTEXT 多以 B-tree 存储）. <https://dev.mysql.com/doc/refman/8.0/en/mysql-indexes.html>
[^mysql-colindex]: MySQL 8.0 Reference Manual — Column Indexes（B-tree 支持 = / > / ≤ / BETWEEN / IN 等点查与区间查）. <https://dev.mysql.com/doc/refman/8.0/en/column-indexes.html>
[^mysql-innodb-index]: MySQL — The Physical Structure of an InnoDB Index / B+Tree index structures in InnoDB（叶子页 level 0 向上递增；树高浅，点查 3~4 次页 I/O）. <https://dev.mysql.com/doc/refman/8.0/en/innodb-physical-structure.html>
[^mysql-rowformat]: MySQL 8.0 Reference Manual — InnoDB Row Formats / Clustered Index（整表 B-tree 即聚簇索引，按主键组织，叶子节点存行数据）. <https://dev.mysql.com/doc/refman/8.0/en/innodb-row-format.html>
[^jusdb-innodb]: JusDB — Understanding InnoDB Architecture: Buffer Pool, Redo Log & Tuning（.ibd 表空间以主键聚簇 B+Tree 存表数据，redo 提供 WAL 崩溃恢复，undo 支撑 MVCC）. <https://www.jusdb.com/blog/understanding-innodb-architecture-performance-reliability-and>
[^ddia-ch3]: Kleppmann — Designing Data-Intensive Applications, 2nd Ed., Ch.3（数据模型：规范化用 ID 引用而非重复人类可读信息）. <https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/ch03.html>
[^ddia-ch7]: Kleppmann — Designing Data-Intensive Applications, 2nd Ed., Ch.7 Transactions（ACID 中 Atomicity = abortability 而非并发；Consistency 属应用层属性）. <https://www.oreilly.com/library/view/designing-data-intensive-applications/9781098119058/>
[^htap-wiki]: Wikipedia — Hybrid transactional/analytical processing（HTAP 由 Gartner 提出，「打破事务与分析之间的墙」，支持实时业务决策）. <https://en.wikipedia.org/wiki/Hybrid_transactional/analytical_processing>
[^htap-survey]: Özcan et al. — Hybrid Transactional/Analytical Processing: A Survey（HTAP 由 Gartner 造词，指在单一系统 / 单一事务内同时支持 OLTP 与 OLAP）. <https://pages.cs.wisc.edu/~yxy/cs839-s20/papers/htap-survey.pdf>
[^htap-arxiv]: HTAP Databases: A Survey (arXiv 2404.15670, 2024)（自 Gartner 造词以来涌现大量 HTAP 数据库，核心挑战是兼顾事务性能与实时分析）. <https://arxiv.org/html/2404.15670v1>
