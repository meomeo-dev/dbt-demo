-- 容器首次初始化时自动执行(挂载到 /docker-entrypoint-initdb.d/)
-- 创建 SQLTools 只读账户:仅用于在 VSCode 里查看表结构 / 调试 SQL。
-- 权限最小化:只对 analytics 库 SELECT + SHOW VIEW,不能写、不能碰其他库。
-- 密码 sqltools_pw 仅为本地 demo 用途,非生产密钥。
CREATE USER IF NOT EXISTS 'sqltools'@'%' IDENTIFIED BY 'sqltools_pw';
GRANT SELECT, SHOW VIEW ON analytics.* TO 'sqltools'@'%';
FLUSH PRIVILEGES;
