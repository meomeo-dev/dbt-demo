# 02 · 连接配置 profiles.yml

> 对应代码:`profiles.yml`

dbt 本身**不连数据库**,它只负责把你写的 SQL 模型编译成目标数仓能执行的 SQL,再通过 **adapter(适配器)** 把 SQL 发给数仓。「连哪个数仓、用什么账号、连哪个库」这些信息,全部写在 `profiles.yml` 里。

## profile / target / output 三个概念

```yaml
# profiles.yml(本项目实际内容)
my_local_dwh:            # ← profile 名,必须和 dbt_project.yml 里的 profile: 对上
  target: dev            # ← 默认用哪个 output(可被 --target 覆盖)
  outputs:
    dev:                 # ← 一个 output = 一套连接配置
      type: trino        # ← 用哪个 adapter(dbt-trino)
      method: none       # 本地无认证(no auth)
      user: dbt          # trino adapter 必填
      host: localhost
      port: 8080
      database: mysql    # Trino catalog 名 = catalog/mysql.properties 文件名
      schema: analytics  # MySQL database 名
      threads: 4         # 并发线程数
```

- **profile**:一个连接方案的命名集合。`dbt_project.yml` 里写 `profile: 'my_local_dwh'`,dbt 就来这里找同名条目。
- **target**:一个 profile 下可以有多套 output(如 `dev` / `prod`),`target:` 指定默认用哪套。跑命令时可用 `dbt run --target prod` 切换。这是**环境分离**的关键(见 [第 10 章](10-步骤8-物化策略与增量.md))。
- **output**:一套具体的连接参数,`type` 决定用哪个 adapter。

## 本项目的特殊性:dbt 从不直连 MySQL

这是本项目最反直觉的一点,务必记住:

```
dbt ──SQL──▶ Trino(计算引擎)──MySQL connector──▶ MySQL(存储)
```

- `type: trino` → dbt 只和 **Trino** 说话,不用 dbt-mysql adapter。
- `database: mysql` → 这里的 `mysql` **不是** "MySQL 数据库" 的意思,而是 **Trino catalog 的名字**,对应 `deploy/trino-config/catalog/mysql.properties` 这个文件名。
- `schema: analytics` → 这才是 MySQL 里真实的 database(库)名。

> 术语错位是初学最大的坑:dbt 语境里的 `database` = Trino catalog,`schema` = MySQL database。记不住就回来看这张表。

| dbt 配置项 | 在本项目里实际指 |
|-----------|-----------------|
| `database: mysql` | Trino catalog `mysql`(即 `mysql.properties`) |
| `schema: analytics` | MySQL 的 `analytics` 库 |

## profiles.yml 放在哪

dbt 按顺序找:先看项目目录下的 `profiles.yml`(本项目就放这儿,方便随仓库分发一份 `method: none` 的参考副本),再看 `~/.dbt/profiles.yml`。

> **安全约束**:生产环境的 `profiles.yml` 含密钥,**绝不能提交到 git**。本项目的副本是 `method: none` 无密钥,才敢入库。真实项目应用环境变量:`password: "{{ env_var('DBT_PASSWORD') }}"`。

## 验证连接

配好后第一件事永远是:

```bash
dbt debug
```

它会检查 `profiles.yml` 语法、adapter 是否装了、以及**能否真正连上** Trino。连不上就别往下走。

---

**上一章**:[01 · dbt 概念与项目结构](01-dbt-概念与项目结构.md) · **下一章**:[03 · 第 1 步 RAW 数据入库](03-步骤1-RAW数据入库-seeds.md)
