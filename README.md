# dbt Core 1.11 + Trino + MySQL 9.7 本地开发环境 — 配置与部署指南

> 面向"日常开发(daily development)":本地一台机器,dbt Core 1.11 通过 **dbt-trino → Trino 482** 连接 MySQL 9.7。
> Trino 充当查询引擎,MySQL 充当数据源/存储层,dbt 在 Trino 上执行 SQL 转换。
> 安装命令以官方文档为基准:[Installing dbt Locally](https://docs.getdbt.com/docs/local/install-dbt)。

---

## 1. 版本与推荐配置

**软件版本**

| 组件 | 版本 | 说明 |
|---|---|---|
| dbt Core | **1.11.12** | Python 线正式版;从 1.12.0 降级以兼容 VSCode Power User 插件(见第 11 章) |
| dbt-trino | **1.10.2** | Starburst 维护;兼容 dbt-core 1.10 / 1.11 / 1.12 |
| Trino | **482** | 2026-06-25 最新稳定版;Docker 镜像 `trinodb/trino:482` |
| MySQL | **9.7 LTS** | Trino MySQL connector 支持 "8.0 or higher";9.7 为当前 MySQL LTS(2026-04) |
| Python | **3.11 / 3.12** | 本项目实测 3.12.5 跑通;选最稳的 3.11 / 3.12 |

**开发机推荐配置**

| 资源 | 推荐 | 最低 | 说明 |
|---|---|---|---|
| 内存(RAM) | **32 GB** | **Linux 8 GB / macOS·Windows 12 GB** | Trino 是 JVM 进程,内存下限由 OS 决定。详见下方「最小内存配置」。 |
| CPU | **8 核** | 4 核 | Trino 是多线程 MPP 引擎;dbt `threads` 消耗并发连接。 |
| 磁盘 | **SSD 40 GB+** | SSD 20 GB | Trino Docker 镜像约 1.5 GB + MySQL 数据目录 + spill 目录,全部走 SSD。 |
| 操作系统 | macOS / Linux / Windows | — | Apple Silicon(ARM64)可直接拉官方多架构镜像。 |

**最小内存配置(100k 行开发数据)**

| OS | 最低 RAM | Trino `-Xmx` | Docker Desktop 分配 | 原因 |
|---|---|---|---|---|
| Linux | **8 GB** | `2G` | 无 VM 开销 | Docker 直接跑,Trino 2.5 GB + MySQL 0.3 GB + OS 2 GB ≈ 5 GB,有余量 |
| macOS / Windows | **12 GB** | `3G` | ≥ 6 GB | Docker Desktop 需要一个 Linux VM,固定消耗 2–4 GB;8 GB 机器装不下 |

> 16 GB 在所有平台上都是舒适的开发线,不需要特别调参。

---

## 2. 架构

```
┌───────────┐    ┌─────────────────────────────────────────────┐
│  开发者    │ -> │  dbt Core 1.11  (Python 编排 / 生成 SQL)    │
└───────────┘    └──────────────────────┬──────────────────────┘
                                        │ dbt-trino (JDBC over HTTP)
                               ┌────────▼────────┐
                               │  Trino 482       │  ← 查询引擎,执行 dbt SQL
                               │  (Docker 容器)   │
                               └────────┬────────┘
                                        │ MySQL connector (JDBC)
                               ┌────────▼────────┐
                               │  MySQL 9.7       │  ← 数据存储层
                               │  (Docker 容器)   │
                               └─────────────────┘
```

- **Trino** 是查询引擎:dbt 把 SQL 发给 Trino,Trino 通过 MySQL connector 在 MySQL 上执行。
- **dbt 视角**:通过 `dbt-trino` 连接 Trino,`database` 对应 Trino **catalog 名**(`mysql`),`schema` 对应 MySQL **database 名**(`analytics`)。
- **全程无需 dbt-mysql**:dbt 只和 Trino 通信,Trino 管 MySQL 连接。

---

## 3. 前置准备

### 3.1 确认 Python

```bash
python3 --version   # 期望 3.11 / 3.12
```

### 3.2 创建项目目录 + 虚拟环境

```bash
mkdir my_dbt_project && cd my_dbt_project
python3 -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
```

### 3.3 确认 Docker + Docker Compose 可用

```bash
docker --version
docker compose version
```

---

## 4. 起 MySQL 9.7 + Trino 482(Docker Compose)

在项目根目录创建 `docker-compose.yml` 和 Trino MySQL catalog 配置文件。

### 4.1 Trino 配置文件

```bash
mkdir -p trino-config/catalog
```

**`trino-config/catalog/mysql.properties`** — MySQL catalog:

```properties
connector.name=mysql
connection-url=jdbc:mysql://mysql:3306
connection-user=dbt
connection-password=dbt_pw
```

> 此文件名 `mysql.properties` 决定了 Trino catalog 名为 `mysql`。`mysql` 是 docker-compose 服务名,在 Docker 内网里即为 MySQL 主机名。

**`trino-config/jvm.config`** — JVM 堆大小(按实际内存调整):

```
-server
-Xmx8G
-XX:+UseG1GC
-XX:G1HeapRegionSize=32M
-XX:+ExplicitGCInvokesConcurrent
-XX:+HeapDumpOnOutOfMemoryError
-XX:+ExitOnOutOfMemoryError
-Djdk.attach.allowAttachSelf=true
-DHADOOP_USER_NAME=trino
```

> **`-Xmx` 按机器内存和 OS 调整**:
> - 32 GB 机器 → `-Xmx8G`(默认)
> - 16 GB 机器 → `-Xmx4G`
> - **8 GB Linux 机器 → `-Xmx2G`**（无 Docker Desktop VM 开销，可跑）
> - **8 GB macOS/Windows → 不可行**（Docker Desktop VM 固定消耗 2–4 GB，改用 12 GB 以上机器）

### 4.2 docker-compose.yml

```yaml
services:
  mysql:
    image: mysql:9.7
    container_name: dbt-mysql
    environment:
      MYSQL_ROOT_PASSWORD: rootpw
      MYSQL_DATABASE: analytics
      MYSQL_USER: dbt
      MYSQL_PASSWORD: dbt_pw
    ports:
      - "3306:3306"
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-udbt", "-pdbt_pw"]
      interval: 10s
      retries: 5

  trino:
    image: trinodb/trino:482
    container_name: dbt-trino
    ports:
      - "8080:8080"
    volumes:
      - ./trino-config/catalog:/etc/trino/catalog
      - ./trino-config/jvm.config:/etc/trino/jvm.config   # JVM 堆大小覆写
    depends_on:
      mysql:
        condition: service_healthy
```

### 4.3 启动并验证

```bash
# 后台启动
docker compose up -d

# 等待 Trino 就绪(首次启动约 30 秒)
docker logs -f dbt-trino | grep "SERVER STARTED"   # Ctrl+C 退出

# 验证 Trino 能查到 MySQL
docker exec -it dbt-trino trino \
  --execute "SHOW SCHEMAS FROM mysql;"
# 应看到 analytics 等 schema
```

（可选,推荐）确保 dbt 用户有权在 MySQL 里创建/删除 schema:

```bash
docker exec -i dbt-mysql mysql -uroot -prootpw \
  -e "GRANT ALL PRIVILEGES ON *.* TO 'dbt'@'%'; FLUSH PRIVILEGES;"
```

> ⚠️ `GRANT ALL ON *.*` 只适合本地一次性开发容器,共享/生产环境按最小权限授予。

---

## 5. 安装 dbt Core 1.11 + dbt-trino

```bash
python -m pip install "dbt-core==1.11.12" "dbt-trino==1.10.2"
```

验证:

```bash
dbt --version
# 应看到 installed: 1.11.x(core),并在 Plugins/adapters 里列出 trino
```

---

## 6. 配置文件

### 6.1 `~/.dbt/profiles.yml`(连接配置)

> dbt 通过 dbt-trino 连 Trino,`database` = Trino catalog 名(`mysql`),`schema` = MySQL database 名(`analytics`)。

```yaml
# ~/.dbt/profiles.yml
my_local_dwh:
  target: dev
  outputs:
    dev:
      type: trino
      method: none              # 本地无认证;生产可改为 ldap/oauth/kerberos
      host: localhost
      port: 8080
      database: mysql           # Trino catalog 名 = mysql.properties 的文件名
      schema: analytics         # MySQL database 名;dbt 模型建到这里
      threads: 4
```

### 6.2 `dbt_project.yml`(项目定义)

```yaml
# dbt_project.yml
name: 'my_project'
version: '1.0.0'
profile: 'my_local_dwh'

model-paths: ["models"]
seed-paths:  ["seeds"]

models:
  my_project:
    +materialized: view
```

---

## 7. 数据源:jaffle-shop 电商数据集

本项目用 dbt 官方经典教学数据集 **jaffle-shop**(一家虚构三明治电商)作为原始数据,共 6 张 `raw_` 表。数据由官方生成器 [`jafgen`](https://pypi.org/project/jafgen/) 生成后采样为教学规模,放在 `seeds/jaffle-data/`,通过 `dbt seed` 载入 MySQL `analytics` schema。

**6 张原始表(seeds/jaffle-data/):**

| 表 | 行数 | 内容 | 关键列 |
|----|------|------|--------|
| `raw_customers` | 50 | 客户 | `id`, `name` |
| `raw_orders` | 300 | 订单(金额单位:分) | `id`, `customer`, `store_id`, `ordered_at`, `subtotal`, `tax_paid`, `order_total` |
| `raw_items` | 537 | 订单行(订单↔商品多对多) | `id`, `order_id`, `sku` |
| `raw_products` | 10 | 商品/菜单 | `sku`, `name`, `type`, `price`, `description` |
| `raw_supplies` | 65 | 供应品/耗材成本 | `id`, `name`, `cost`, `perishable`, `sku` |
| `raw_stores` | 6 | 门店 | `id`, `name`, `opened_at`, `tax_rate` |

**如何重新生成数据(可选):**

```bash
pip install jafgen
jafgen 1                       # 模拟 1 年;输出到 ./jaffle-data/
# 数据量较大(orders 7 万行),教学项目已采样为 50 客户 / 300 订单
```

> 采样时保证了**引用完整性**:`orders.customer ⊆ customers`、`items.order_id ⊆ orders`、`items.sku ⊆ products`,因此 `relationships` 外键测试全部可通过。

> seed 的列类型在 `dbt_project.yml` 里显式声明(金额为 integer、perishable 为 varchar),避免 Trino/MySQL 对 CSV 的类型推断出错。

---

## 8. 项目目录结构

```
dbt-demo/
├── .venv/                          # 虚拟环境(不进版本库)
├── deploy/
│   └── docker-compose.yml          # MySQL + Trino 本地服务编排
├── dbt_project.yml                 # dbt 项目定义(seed 类型 / 分层物化)
├── profiles.yml                    # 连接配置(database=mysql catalog, schema=analytics)
├── seeds/
│   └── jaffle-data/                # 6 张 jaffle-shop 原始 CSV
│       ├── raw_customers.csv       raw_orders.csv     raw_items.csv
│       └── raw_products.csv        raw_supplies.csv   raw_stores.csv
├── models/
│   ├── staging/                    # 清洗层(stg_,一对一映射源表)
│   │   ├── sources.yml             #    6 张 source 声明 + source 层测试
│   │   ├── stg_schema.yml          #    staging 模型文档与测试
│   │   ├── stg_customers.sql       stg_orders.sql      stg_order_items.sql
│   │   └── stg_products.sql        stg_supplies.sql    stg_stores.sql
│   ├── intermediate/               # 中间层(int_,JOIN/聚合/改粒度)
│   │   ├── schema.yml
│   │   ├── int_supplies_aggregated_per_product.sql   # 按产品聚合成本
│   │   ├── int_order_items_enriched.sql       # 打平订单行 + 产品 + 成本
│   │   ├── int_order_items_aggregated_to_order.sql   # 订单行→订单聚合
│   │   └── int_orders_aggregated_to_customer.sql     # 订单→客户聚合
│   └── marts/                      # 业务层(星型模型 fct_/dim_)
│       ├── schema.yml
│       ├── dim_customers.sql       dim_products.sql    dim_stores.sql
│       ├── fct_orders.sql          fct_order_items.sql
│       └── fct_orders_incremental.sql          # 增量模型(append 策略)
└── tests/
    └── assert_order_total_positive.sql         # singular test(单点测试)
```

`profiles.yml` 也可放 `~/.dbt/profiles.yml`;本项目放在项目根便于版本管理。

详细的代码结构与教学文档映射见 **[第 12 节:项目代码结构详解](#12-项目代码结构详解与教学文档映射)**。

---

## 9. 跑起来(命令流)

```bash
# 0. 激活虚拟环境,确认服务运行中
source .venv/bin/activate
docker compose up -d

# 1. 检查连通性与配置
dbt debug

# 2. 载入种子数据
dbt seed

# 3. 构建模型
dbt run

# 4. 运行数据测试
dbt test

# 一步到位:本项目请分步执行 seed → run → test(见下方「常见问题」时序说明),
# 不要直接用 dbt build:source 测试不依赖 seed,并发下会偶发 TABLE_NOT_FOUND。

# 生成并预览文档 + 血缘图
dbt docs generate
dbt docs serve
```

常用选择器:

```bash
dbt run --select staging
dbt run --select stg_orders+
dbt run --target prod
```

停止本地服务:

```bash
docker compose down
```

---

## 10. 验证与常见问题

- **`dbt debug` 报 Connection refused(8080)**:Trino 还没起好。`docker logs dbt-trino | tail -20` 确认状态,首次启动约需 30 秒。
- **`SHOW SCHEMAS FROM mysql` 在 Trino 里报错**:检查 `trino-config/catalog/mysql.properties` 是否挂载进容器(`docker exec dbt-trino ls /etc/trino/catalog/`)。
- **Trino 能连但 MySQL 拒绝(Access denied)**:回到 4.3 执行那条 `GRANT`,或确认 `mysql.properties` 里的用户名/密码与 MySQL 容器环境变量一致。
- **`dbt seed` 或 `dbt run` 报 catalog `mysql` 不存在**:确认 `profiles.yml` 里 `database: mysql` 与 catalog 文件名 `mysql.properties` 一致(去掉 `.properties` 后缀)。
- **`schema` 找不到**:Trino 里的 schema = MySQL database。确认 MySQL 容器里有 `analytics` 这个 database(Docker 环境变量 `MYSQL_DATABASE=analytics` 会自动建)。
- **改了 CSV 但表没变**:`dbt seed --full-refresh`。
- **想每次干净重建**:分步 `dbt seed --full-refresh && dbt run && dbt test`。

### dbt-trino 专属踩坑(本 demo 已规避)

- **`This connector does not support creating views`**:Trino MySQL connector **不支持 `CREATE VIEW`**。因此 `dbt_project.yml` 里 staging 层必须用 `+materialized: table`,不能用 `view`(dbt 默认 staging 常配 view,这里会直接报错)。
- **`dbt build` 偶发 `TABLE_NOT_FOUND` / `Table 'analytics.xxx' not found`**:`dbt build` 把 seed 和 source 测试编排进同一 DAG 并发执行,但 **source 测试在语义上不依赖 seed**(source 代表"外部已存在的表"),于是测试可能在 seed 建表的瞬间抢跑,触发 Trino 对 MySQL DDL 可见性的瞬时竞态。**解法**:本地 demo 用分步 `dbt seed → dbt run → dbt test`,不要用 `dbt build`。生产中 source 本就是外部真实表,不存在此问题。
- **`Column 'c.xxx' cannot be resolved`(JOIN 后)**:Trino 里 `LEFT JOIN ... USING (col)` 会把连接列合并成**单一无别名列**,之后不能再用 `c.col` / `o.col` 引用。改用显式 `LEFT JOIN ... ON c.col = o.col`。
- **`Docker mounts denied: path is not shared`**:Docker Desktop 开启 VirtioFS 后只挂载白名单路径。本 demo 通过 `deploy/Dockerfile.trino` 把 catalog/jvm 配置打进镜像(而非 volume mount)规避,`docker compose build trino` 即可。

---

## 11. VSCode 开发插件安装与配置

本项目推荐用 **Power User for dbt**(`innoverio.vscode-dbt-power-user`)做本地开发:提供 model 自动补全、编译预览(compiled SQL)、血缘图(lineage)、一键 run/test 等能力。它是**引擎无关(engine-agnostic)**的,原生支持 dbt Core 1.x + 任意 adapter(含 dbt-trino)。

> 注意:不要用 dbt Labs 官方扩展(`dbtlabsinc.dbt`)。它强依赖 **dbt Fusion 引擎**(dbt Core 2.0),而 Fusion 目前**不支持 Trino/Starburst adapter**,无法连本项目的 Trino。

### 11.1 安装扩展

在 VSCode 扩展面板(`Cmd+Shift+X`)搜索并安装:

- **Power User for dbt**(`innoverio.vscode-dbt-power-user`)— 核心
- **Python**(`ms-python.python`)— Power User 的硬依赖,负责解释器管理
- **jinjahtml**(`samuelcolvin.jinjahtml`)— `.sql` 的 Jinja 语法高亮(可选)

### 11.2 让插件用项目的 `.venv`(关键)

Power User 通过 Python 扩展选定的解释器来调用 dbt。若解释器指错(如指向全局 pyenv / Homebrew Python),插件会**静默空白**或报错。两步确保它用项目 `.venv`:

1. `Cmd+Shift+P` → `Python: Select Interpreter` → 选 `.venv/bin/python`。
2. 在 `.vscode/settings.json` 里用 `dbt.dbtPythonPathOverride` **强制**锁定,即使 Python 扩展选了别的解释器也不受影响。

本项目 `.vscode/settings.json` 已配置好以下 Power User 参数(均指向 `.venv`):

```jsonc
{
  // 强制 Power User 使用 .venv 里的 Python(内含 dbt 1.11 + dbt-trino)
  "dbt.dbtPythonPathOverride": "<项目绝对路径>/.venv/bin/python",
  // SQL 格式化工具 sqlfmt(需 pip install shandy-sqlfmt 装进 .venv)
  "dbt.sqlFmtPath": "<项目绝对路径>/.venv/bin/sqlfmt",
  // 只把项目根作为 dbt 项目纳入(workspace 相对路径)
  "dbt.allowListFolders": ["."]
}
```

> `python.defaultInterpreterPath` **不支持** `${workspaceFolder}` 变量,必须写绝对路径。
> sqlfmt 不随 dbt 安装,需单独 `pip install shandy-sqlfmt`(纯格式化工具,无风险)。

改完 `settings.json` 后执行 `Developer: Reload Window` 让插件重新加载。

### 11.3 为什么钉在 dbt 1.11(而非 1.12)

Power User(截至 0.62.0)的 Python bridge 在序列化 dbt **1.12** 的某些返回对象时会崩溃:

```
TypeError: must be real number, not _thread.lock
```

根因是插件 `node_python_bridge.py` 的 `JavaScriptEncoder.default()` 未对未知对象类型(线程锁)做兜底处理——这是**插件缺陷**,与本项目配置无关。dbt 回到插件充分测试过的 **1.11.12** 可规避该触发条件。待插件新版修复后,可参考备份的依赖快照升回 1.12。

### 11.4 常见问题

- **插件空白、无 model**:解释器没指向 `.venv`。按 11.2 重选并 Reload。
- **`_thread.lock` 报错**:dbt 版本触发的插件 bug,见 11.3(已通过降级 1.11 规避)。
- **血缘图 / 补全不更新**:manifest 过期。在项目根跑 `dbt parse` 重新生成 `target/manifest.json`。
- **不要装 dbt Labs 官方扩展**:见本章开头说明,它需要 Fusion,连不上 Trino。

---


- [Connect Starburst/Trino to dbt Core](https://docs.getdbt.com/docs/core/connect-data-platform/trino-setup) — dbt-trino profile 字段
- [starburstdata/dbt-trino (GitHub)](https://github.com/starburstdata/dbt-trino) — 适配器源码与 CHANGELOG
- [dbt-trino (PyPI)](https://pypi.org/project/dbt-trino/) — 1.10.2 正式版
- [Trino MySQL connector 官方文档](https://trino.io/docs/current/connector/mysql.html) — catalog 配置与 JDBC URL 格式
- [Trino in a Docker container](https://trino.io/docs/current/installation/containers.html) — Docker 部署说明
- [dbt-core (PyPI)](https://pypi.org/project/dbt-core/) — 1.11.12 正式版
- [Power User for dbt (VSCode Marketplace)](https://marketplace.visualstudio.com/items?itemName=innoverio.vscode-dbt-power-user) — 引擎无关的 dbt 开发插件

---

## 12. 项目代码结构详解与教学文档映射

本项目的每一层代码都对应 `docs/dbt-数仓教学/` 的一章教学文档。数据流(data flow)完整链路:

```
seeds(RAW)  →  sources  →  staging(stg_)  →  intermediate(int_)  →  marts(fct_/dim_)  →  tests / docs
   6 CSV        6 声明        6 清洗模型          4 转换模型            2 事实 + 3 维度       67 测试
```

> marts 层除「2 事实 + 3 维度」外,另有 1 张增量演示表 `fct_orders_incremental`(append 策略,见 12.3)。

### 12.1 数据流全景(DAG)

```
raw_customers ─→ stg_customers ───────────────────────────────────────┐
raw_orders ────→ stg_orders ──┬──────────────────────────────┐         │
raw_items ─────→ stg_order_items ─┐                           │         │
raw_products ──→ stg_products ────┤                           ▼         ▼
raw_supplies ──→ stg_supplies ─→ int_supplies_aggregated_per_product    │
raw_stores ────→ stg_stores       │            │                        │
                                  ▼            ▼                        │
                       int_order_items_enriched ──────────────┐  int_orders_aggregated_to_customer
                                  │        │                  │         │
                                  │        ▼                  │         ▼
                                  │  int_order_items_aggregated_to_order │
                                  │        │                  │         │
                                  ▼        ▼                  ▼         ▼
                          fct_order_items  fct_orders   dim_products  dim_customers
                                                                (← int_supplies_agg)
                                                        dim_stores (← stg_stores)
```

> 重构后:改变粒度的聚合(订单→客户、订单行→订单)已从 marts 下沉到 intermediate。`dim_customers` 引用 `int_orders_aggregated_to_customer`,`fct_orders` 引用 `int_order_items_aggregated_to_order`(不再依赖 `fct_order_items`,消除了 fact→fact 依赖)。marts 层只做 JOIN 组装,不再内联 `group by`。

### 12.2 逐模块说明

**`seeds/jaffle-data/` — RAW 原始数据**(教学文档:`03-步骤1-RAW数据入库-seeds.md`)
`dbt seed` 把 6 张 CSV 载入 MySQL `analytics` schema,是整条链路的起点。列类型在 `dbt_project.yml` 中显式声明。

**`models/staging/` — 清洗层 stg_**(教学文档:`04-步骤2-声明数据源-sources.md`、`05-步骤3-staging清洗层.md`)

| 文件 | 职责 |
|------|------|
| `sources.yml` | 声明 6 张 `raw` source 表 + 在源头挂 unique/not_null/relationships/accepted_values 测试 |
| `stg_customers.sql` | 重命名(一对一映射,不 JOIN 不聚合) |
| `stg_orders.sql` | 重命名 + 金额分转元 + datetime 转 date(基础计算) |
| `stg_order_items.sql` | 订单行重命名 |
| `stg_products.sql` | 重命名 + 金额转换 + 分类归桶(`is_food_item`) |
| `stg_supplies.sql` | 类型转换:字符串 `'True'/'False'` → boolean |
| `stg_stores.sql` | 重命名 + 日期转换 |
| `stg_schema.yml` | staging 模型的文档与测试 |

**`models/intermediate/` — 中间层 int_**(教学文档:`06-步骤4-intermediate中间层.md`)

| 文件 | 模式 | 职责 |
|------|------|------|
| `int_supplies_aggregated_per_product.sql` | 聚合(改粒度) | 多条供应品 → 每产品一行的总成本 |
| `int_order_items_enriched.sql` | JOIN | 打平订单行,补齐产品售价、成本、毛利 |
| `int_order_items_aggregated_to_order.sql` | 聚合(改粒度) | 多条订单行 → 每订单一行的商品数/成本/毛利 |
| `int_orders_aggregated_to_customer.sql` | 聚合(改粒度) | 多笔订单 → 每客户一行的累计订单/消费/首末单日期 |

staging 禁止的 JOIN/聚合都落在这一层。命名遵循官方 `int_[entity]s_[verb]s` 约定。**改变粒度的聚合一律放 int**,marts 的 `dim_`/`fct_` 只做 JOIN 组装(不再内联 `group by`);其中 `int_order_items_aggregated_to_order` 从 `int_order_items_enriched` 聚合而非 `fct_order_items`,以消除 fact→fact 依赖。

**`models/marts/` — 业务层(星型模型)**(教学文档:`07-步骤5-marts业务聚合层.md`、`00-数仓基础-ETL-vs-ELT.md`)

| 文件 | 类型 | 粒度 | 说明 |
|------|------|------|------|
| `dim_customers.sql` | 维度表 | 客户 | 纯 JOIN 组装:客户属性 + 引用 int 层累计订单/消费/首末单日期 |
| `dim_products.sql` | 维度表 | 产品 | 含售价/成本/毛利 |
| `dim_stores.sql` | 维度表 | 门店 | 门店属性 |
| `fct_orders.sql` | 事实表 | 订单 | 纯 JOIN 组装:金额度量 + 引用 int 层订单行汇总(商品数/成本/毛利) |
| `fct_order_items.sql` | 事实表 | 订单行 | 最细粒度,外键指向各维度 |
| `fct_orders_incremental.sql` | 增量事实表 | 订单 | 演示 incremental 物化(见 12.3) |
| `schema.yml` | — | — | marts 模型文档 + 主键/外键测试 |

所有 JOIN 用**显式 `ON`**(不用 `USING`)——Trino 对 `USING` 支持有限,这是本项目的硬约定。

**`tests/` — 单点测试**(教学文档:`08-步骤6-测试与数据质量.md`)
`assert_order_total_positive.sql` 是 singular test:返回"订单总额为负"的行,返回 0 行即通过。四大通用测试(unique/not_null/accepted_values/relationships)写在各 `schema.yml`。

### 12.3 平台约束:incremental 与 snapshot 的实测结论

教学文档 `10-步骤8-物化策略与增量.md` 讲了五种物化。**本项目实测**了在 Trino → MySQL connector 上的可行性,结论如下(重要,与官方标准环境不同):

| 特性 | 官方标准环境 | 本项目(Trino MySQL connector) | 原因 |
|------|-------------|-------------------------------|------|
| view 物化 | ✅ 默认 | ❌ 改用 table | connector 不支持 `CREATE VIEW` |
| table 物化 | ✅ | ✅ | — |
| incremental `append` | ✅ | ✅ **可跑**(`fct_orders_incremental`) | 纯 `INSERT`,不触发 MERGE |
| incremental `merge` | ✅ | ❌ | connector 不支持事务性 MERGE |
| incremental `delete+insert` | ✅ | ❌ | Trino 的 `DELETE...WHERE IN` 在此 connector 上走 row-level MERGE |
| snapshot(SCD2) | ✅ | ❌ 仅首次建表可成功,更新失败 | 变更检测依赖 MERGE |

因此本项目的 `fct_orders_incremental.sql` **强制**使用:
- `incremental_strategy='append'`(唯一可跑的策略)
- `views_enabled=false`(临时中间关系用 table 而非 view)

snapshot 因无法完整跑通,未纳入可运行代码,其原理与示例见教学文档第 10 章。若需在本环境真正使用 incremental merge / snapshot,需改用支持 MERGE 的 connector(Hive/Iceberg/Delta Lake)。

### 12.4 一键复现

```bash
source .venv/bin/activate
dbt seed --full-refresh   # 6 seeds
dbt run                   # 16 models(全部 table 物化)
dbt test                  # 67 tests 全绿
dbt docs generate         # 生成文档与血缘图
```

实测结果:seed 6 ✅ / run 16 ✅ / test 67 ✅ / 0 error。

### 12.5 教学文档索引

完整速成课程见 [`docs/dbt-数仓教学/`](docs/dbt-数仓教学/README.md),14 章覆盖从数仓理论到本项目每一层代码,含权威来源(经交叉验证的 dbt 官方文档 URL)。
