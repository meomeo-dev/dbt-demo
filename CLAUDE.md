# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概览

本地 dbt 开发 demo:**dbt Core 1.11 → dbt-trino → Trino 482 → MySQL 9.7**。dbt 只与 Trino 通信,由 Trino 通过 MySQL connector 在 MySQL 上执行 SQL。全程不使用 dbt-mysql adapter。

- `database` = Trino **catalog 名**(`mysql`,对应 `deploy/trino-config/catalog/mysql.properties` 文件名)
- `schema` = MySQL **database 名**(`analytics`)

## 常用命令

先起后端(Docker),再跑 dbt。dbt 命令需在已激活项目虚拟环境、且 profile 可被读取的前提下执行。

```bash
# 起后端(Trino + MySQL),首次启动 Trino 约需 30 秒
cd deploy && docker compose up -d && cd ..
docker compose -f deploy/docker-compose.yml logs -f trino | grep "SERVER STARTED"

# 检查连通性与配置
dbt debug

# 分步跑:seed → run → test(顺序不可省,原因见下)
dbt seed --full-refresh
dbt run
dbt test

# 单个模型 / 单个测试
dbt run  --select stg_customers
dbt test --select customer_orders
dbt build --select stg_customers   # 单模型可用 build,全量不要用(见下)

# 改了 seed CSV 但表没更新
dbt seed --full-refresh

# 生成并预览文档 + 血缘图
dbt docs generate && dbt docs serve
```

## 关键约束(dbt-trino 专属,违反会直接报错或偶发失败)

这些是本项目区别于普通 dbt 项目的核心陷阱,修改模型/配置前必须知道:

1. **staging 层必须用 `table` 物化,不能用 `view`**。Trino MySQL connector 不支持 `CREATE VIEW`。`dbt_project.yml` 已对 staging 和 marts 统一设 `+materialized: table`。新增模型层时沿用 table。

2. **全量构建用分步 `seed → run → test`,不要用 `dbt build`**。`dbt build` 把 seed 与 source 测试编排进同一并发 DAG,但 source 测试语义上不依赖 seed,会在 seed 建表瞬间抢跑,触发 Trino 对 MySQL DDL 可见性的瞬时竞态,偶发 `TABLE_NOT_FOUND`。单模型的 `dbt build --select x` 无此问题。

3. **JOIN 不要用 `USING`,改用显式 `ON`**。Trino 里 `LEFT JOIN ... USING (col)` 会把连接列合并为单一无别名列,之后 `c.col` / `o.col` 无法解析。参见 `models/marts/customer_orders.sql`。

## 数据流架构

三层,全部 table 物化:

- `models/staging/sources.yml` — 声明 source:Trino catalog `mysql` 下 `analytics` schema 的 seed 表(`raw_customers` / `raw_orders`)
- `models/staging/stg_*.sql` — 清洗层,一对一映射 source,重命名/类型规整
- `models/marts/customer_orders.sql` — 聚合层,JOIN staging 模型产出客户订单宽表

seeds(`seeds/*.csv`)通过 `dbt seed` 载入 MySQL 的 `analytics` 库,充当"外部数据源"被 staging 引用。

## 版本约束

**dbt 固定在 1.11.x,不要升级到 1.12。** VSCode 的 Power User 插件(≤0.62.0)的 Python bridge 在序列化 dbt 1.12 返回对象时崩溃(`TypeError: must be real number, not _thread.lock`),这是插件缺陷。1.11.12 为规避版本。详见 README 第 11 章。dbt-trino 用 1.10.2(兼容 core 1.10/1.11/1.12)。

## 环境相关(不在版本库中,按本机配置)

- 虚拟环境、Python 解释器路径因开发者机器而异;dbt 命令须在项目虚拟环境内运行。
- `.vscode/settings.json` 里 `dbt.dbtPythonPathOverride` / `dbt.sqlFmtPath` 用了 `<ABSOLUTE_PROJECT_PATH>` 占位符(Power User 插件不支持 `${workspaceFolder}` 变量),克隆后需替换为本机绝对路径。
- `profiles.yml` 为本地参考副本(`method: none`,无密钥);dbt 也会读 `~/.dbt/profiles.yml`。
- SQL 格式化用 sqlfmt,需 `pip install shandy-sqlfmt` 装进虚拟环境。
