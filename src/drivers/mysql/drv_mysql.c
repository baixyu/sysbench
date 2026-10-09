/* Copyright (C) 2004 MySQL AB
   Copyright (C) 2004-2017 Alexey Kopytov <akopytov@gmail.com>

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; either version 2 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/

#ifdef STDC_HEADERS
# include <stdio.h>
#endif
#ifdef HAVE_CONFIG_H
# include "config.h"
#endif
#ifdef _WIN32
#include <winsock2.h>
#endif

#ifdef HAVE_STRING_H
# include <string.h>
#endif
#ifdef HAVE_STRINGS_H
# include <strings.h>
#endif
#include <stdio.h>
#include <unistd.h>

#include <mysql.h>
#include <mysqld_error.h>
#include <errmsg.h>

#include "sb_options.h"
#include "db_driver.h"

#define DEBUG(format, ...)                      \
  do {                                          \
    if (SB_UNLIKELY(args.debug != 0))           \
      log_text(LOG_DEBUG, format, __VA_ARGS__); \
  } while (0)

#define SAFESTR(s) ((s != NULL) ? (s) : "(null)")

#if !defined(MARIADB_BASE_VERSION) && !defined(MARIADB_VERSION_ID) && \
  MYSQL_VERSION_ID >= 80001 && MYSQL_VERSION_ID != 80002 /* see https://bugs.mysql.com/?id=87337 */
typedef bool my_bool;
#endif

/* MySQL driver arguments */

static sb_arg_t mysql_drv_args[] =
{
  SB_OPT("mysql-host", "MySQL server host", "localhost", LIST),
  SB_OPT("mysql-port", "MySQL server port", "3306", LIST),
  SB_OPT("mysql-socket", "MySQL socket", NULL, LIST),
  SB_OPT("mysql-user", "MySQL user", "sbtest", STRING),
  SB_OPT("mysql-password", "MySQL password", "", STRING),
  SB_OPT("mysql-db", "MySQL database name", "sbtest", STRING),
  SB_OPT("mysql-ssl", "use SSL connections, if available in the client "
         "library", "off", BOOL),
  SB_OPT("mysql-ssl-cipher", "use specific cipher for SSL connections", "",
         STRING),
  SB_OPT("mysql-compression", "use compression, if available in the "
         "client library", "off", BOOL),
  SB_OPT("mysql-debug", "trace all client library calls", "off", BOOL),
  SB_OPT("mysql-ignore-errors", "list of errors to ignore, or \"all\"",
         "1213,1020,1205", LIST),
  SB_OPT("mysql-dry-run", "Dry run, pretend that all MySQL client API "
         "calls are successful without executing them", "off", BOOL),

  SB_OPT_END
};

typedef struct
{
  sb_list_t          *hosts;
  sb_list_t          *ports;
  sb_list_t          *sockets;
  char               *user;
  char               *password;
  char               *db;
  unsigned char      use_ssl;
  char               *ssl_cipher;
  unsigned char      use_compression;
  unsigned char      debug;
  sb_list_t          *ignored_errors;
  unsigned int       dry_run;
} mysql_drv_args_t;

typedef struct
{
  MYSQL        *mysql;
  char         *host;
  char         *user;
  char         *password;
  char         *db;
  unsigned int port;
  char         *socket;
  /*
    Percona Raft（本 fork 的用途）：受管写要求每笔事务带请求身份。
    连接建立后探测服务端是否有 percona_raft_request_*（只有我们的构建有），
    有则 namespace 每连接设一次（并逐连接探测：多线程下不能用"进程内只探一次"的闩锁，
    见 raft_setup_identity() 里的说明），request_id 每笔事务换一个新值（见 raft_set_request_id()）。
  */
  unsigned char      managed_identity;   /* 身份变量存在且 namespace 已设 */
  unsigned char      txn_open;           /* 本连接当前有一笔事务开着（显式或隐式） */
  unsigned char      target_mismatch;    /* --target=raft 但服务端没有身份变量 */
  unsigned char      read_consistency_failed;  /* 要强读但服务端设不上（非 Raft 实例） */
  unsigned long long txn_seq;            /* 本连接已开始的受管事务数 */
} db_mysql_conn_t;

/*
  ---- Percona Raft 身份注入与错误分栏（perf-tool fork 专有）---------------
  依据：percona-server 的 Docs/raft_replication/perf_tool_plan.md §7.1 / §9。
  两条规矩都由这里保证：
    * 每笔事务的身份 = namespace（每连接一次）+ request_id（每笔换新），
      且在 `BEGIN` 之前设好 —— 缺任何一条，受管闸门都会按 8109 拒掉整笔事务；
    * 错误码按【数值】分栏（3098/3197/1402 与 8100-8115），不按错误文本匹配。
  为什么放在 C 层：原版 `oltp_*.lua` 不设身份，注入放在驱动里它们才能"原样跑"
  （实测背书：perf_tool_plan.md §7.2）。
------------------------------------------------------------------------- */

static unsigned char      raft_identity_available;   /* 有过连接探到身份变量（报告用） */
static unsigned long long raft_conns_total;
static unsigned long long raft_conns_with_identity;
static unsigned int       raft_conn_seq;
static unsigned long long raft_request_ids_issued;
static unsigned long long raft_cls_refused;
static unsigned long long raft_cls_ambiguous;
static unsigned long long raft_cls_temporary;
static unsigned long long raft_cls_needs_action;
static unsigned long long raft_implicit_txns;      /* 驱动替脚本开的自动提交事务数 */
static unsigned long long raft_conns_read_consistency;  /* 真正设上读契约的连接数 */
static unsigned long long raft_cls_rolled_back_xa;

static void raft_bump(unsigned long long *counter)
{
  __atomic_fetch_add(counter, 1, __ATOMIC_RELAXED);
}

/* 执行一条控制面 SQL 并吃掉结果集（身份注入用；失败只记日志） */
static int raft_run_sql(MYSQL *con, const char *sql)
{
  MYSQL_RES *res;

  if (mysql_query(con, sql) != 0)
    return 1;

  res = mysql_store_result(con);
  if (res != NULL)
    mysql_free_result(res);

  return 0;
}

/* 服务端有没有身份变量（只探一次：同一进程内服务端不会变） */
static int raft_probe_identity(MYSQL *con)
{
  MYSQL_RES *res;
  int found;

  if (mysql_query(con, "SHOW VARIABLES LIKE 'percona_raft_request_namespace'") != 0)
    return 0;

  res = mysql_store_result(con);
  if (res == NULL)
    return 0;

  found = mysql_num_rows(res) > 0;

  mysql_free_result(res);

  return found;
}

/* 连接建立后调用：探测 + 设 namespace（每连接一次） */
static void raft_setup_identity(db_mysql_conn_t *db_mysql_con)
{
  MYSQL *con = db_mysql_con->mysql;
  char   sql[160];
  unsigned int seq;

  if (con == NULL)
    return;

  /*
    强读档（M1-3）：**必须放在身份探测之前**。
    先前放在"探测到身份变量"之后 ⇒ 对着没有这些变量的服务端（原生 MySQL）
    这一段根本不执行：SET 没发、也没有报错，而报告照样打印 LINEARIZABLE
    —— 正是本节声称要防的"静默降级"（实测踩到：43300 上一点动静都没有）。
    服务端自己的规矩也是"宁可报错也不静默降级"（`sys_vars.cc:7470-7480`），
    工具这一侧照同一条走：设不上就让这台连接失败。
  */
  if (db_globals.read_consistency_explicit)
  {
    char rc_sql[96];

    snprintf(rc_sql, sizeof(rc_sql),
             "SET SESSION percona_raft_read_consistency = '%s'",
             db_globals.read_linearizable ? "LINEARIZABLE" : "STALE");

    if (raft_run_sql(con, rc_sql) != 0)
    {
      log_text(LOG_FATAL,
               "Percona Raft: failed to set read consistency to %s: %s.  "
               "LINEARIZABLE needs a server started with --percona-raft-enabled; "
               "refusing to run a strong-read arm that would silently degrade to "
               "local reads.",
               db_globals.read_linearizable ? "LINEARIZABLE" : "STALE",
               mysql_error(con));
      db_mysql_con->read_consistency_failed = 1;
      return;
    }

    __atomic_fetch_add(&raft_conns_read_consistency, 1, __ATOMIC_RELAXED);
    log_text(LOG_INFO, "Percona Raft: session read consistency = %s",
             db_globals.read_linearizable ? "LINEARIZABLE" : "STALE");
  }

  /*
    --target 分档（§4.0）：
      raft  = 断言这是受管集群：探测不到身份变量就**立刻失败**——否则"跑错了目标"
              会被读成性能结论（对原生 MySQL 跑 raft 档，写语句根本不受管，
              报告里 unmanaged 增量是 0，但那个 0 毫无意义）。
      mysql = 原生 MySQL：连探测都不做（省一次往返），纯 stock 行为。
      auto  = 按能力探测（默认）。
  */
  if (db_globals.target == DB_TARGET_MYSQL)
  {
    log_text(LOG_DEBUG, "Percona Raft: --target=mysql, no capability probe, "
                        "no identity injection");
    return;
  }

  /*
    为什么每个连接各探一次、而不是"进程内只探一次"：
    先前用 `raft_identity_probed` 做闩锁，但它是"先发布闩锁、后写探测结果"——
    第二个线程会看到 probed==1 而 available 仍是 0，于是**整条连接不设 namespace**，
    它的写语句就落到服务端的"未纳管"拒绝上（8109）。多线程下这是必然踩到的竞态，
    实测 `--threads=2` 时两个连接里只有一个拿到身份。
    连接是每线程建一次的（不是每事务），所以每连接探一次的成本可以忽略。
  */
  __atomic_fetch_add(&raft_conns_total, 1, __ATOMIC_RELAXED);

  if (!raft_probe_identity(con))
  {
    if (db_globals.target == DB_TARGET_RAFT)
    {
      log_text(LOG_FATAL,
               "Percona Raft: --target=raft but this server has no "
               "percona_raft_request_namespace/percona_raft_request_id.  "
               "Either the target is not our build (use --target=mysql), or this "
               "is not the mode under test (use --target=auto).");
      db_mysql_con->target_mismatch = 1;
      return;
    }

    log_text(LOG_DEBUG, "Percona Raft: no percona_raft_request_* on this server; "
                        "no identity injected (native target)");
    return;
  }

  __atomic_store_n(&raft_identity_available, 1, __ATOMIC_RELAXED);


  seq = __atomic_fetch_add(&raft_conn_seq, 1, __ATOMIC_RELAXED) + 1;

  /*
    身份分量规则（txn_envelope.h）：1..256 UTF-8 字节、不含 NUL、必须 NFC。
    这里用纯 ASCII，天然满足；带上 pid 与连接序号，便于在服务端日志里定位。
  */
  snprintf(sql, sizeof(sql),
           "SET SESSION percona_raft_request_namespace = 'perf-%u-%u'",
           (unsigned) getpid(), seq);

  if (raft_run_sql(con, sql) == 0)
  {
    db_mysql_con->managed_identity = 1;
    __atomic_fetch_add(&raft_conns_with_identity, 1, __ATOMIC_RELAXED);
  }
  else
    log_text(LOG_WARNING, "Percona Raft: failed to set request namespace: %s",
             mysql_error(con));
}

/* 每笔事务开始前调用：换一个新的 request_id */
static void raft_set_request_id(db_mysql_conn_t *db_mysql_con)
{
  char sql[96];

  if (db_mysql_con == NULL || !db_mysql_con->managed_identity)
    return;

  snprintf(sql, sizeof(sql), "SET SESSION percona_raft_request_id = 'txn-%llu'",
           ++db_mysql_con->txn_seq);

  if (raft_run_sql(db_mysql_con->mysql, sql) == 0)
    raft_bump(&raft_request_ids_issued);
  else
    log_text(LOG_WARNING, "Percona Raft: failed to set request id: %s",
             mysql_error(db_mysql_con->mysql));
}

/* 取语句首词（小写，跳过前导空白，遇空白/分号即止）：够用于判别 BEGIN/COMMIT/写语句 */
static void raft_first_word(const char *query, size_t len, char *word, size_t cap)
{
  size_t i = 0;
  size_t n = 0;

  word[0] = '\0';
  if (query == NULL || cap < 2)
    return;

  while (i < len && (query[i] == ' ' || query[i] == '\t' ||
                     query[i] == '\n' || query[i] == '\r'))
    i++;

  for (; i < len && n < cap - 1; i++)
  {
    char c = query[i];

    if (c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == ';')
      break;
    if (c >= 'A' && c <= 'Z')
      c = (char) (c - 'A' + 'a');
    word[n++] = c;
  }
  word[n] = '\0';
}

/* 这条语句是不是事务开始（BEGIN / START TRANSACTION）；"START TRANSACTION" 取首词即可判别 */
static int raft_is_txn_start(const char *query, size_t len)
{
  char word[16];

  raft_first_word(query, len, word, sizeof(word));

  return strcmp(word, "begin") == 0 || strcmp(word, "start") == 0;
}

/* 事务结束（脚本自己收尾）——用于跟踪本连接上还有没有开着的事务 */
static int raft_is_txn_end(const char *query, size_t len)
{
  char word[16];

  raft_first_word(query, len, word, sizeof(word));

  return strcmp(word, "commit") == 0 || strcmp(word, "rollback") == 0;
}

/*
  写语句判定：**只用于"要不要替脚本开一笔自动提交事务"**，与准入判定无关。
  受管契约本身由服务端判（单表 + 显式 BEGIN + 身份），这里只是把自动提交翻译成等价形状。
*/
static int raft_is_write_statement(const char *query, size_t len)
{
  char word[16];

  raft_first_word(query, len, word, sizeof(word));

  return strcmp(word, "insert") == 0 || strcmp(word, "update") == 0 ||
         strcmp(word, "delete") == 0 || strcmp(word, "replace") == 0;
}

/*
  自动提交包装（M1-1）。
  背景（实测）：sysbench 的组合脚本自己发 BEGIN（`oltp_read_write.lua` 等），但
  `oltp_insert.lua` / `oltp_delete.lua` / `oltp_update_*.lua` 的 event() **只有一条语句**，
  靠自动提交——它们在受管集群上会被 8109 拒。给这类语句各包一笔受管事务，
  语义与自动提交**完全一致**（一条语句一笔事务），因此不必改脚本、也不改变压测语义。

  返回：
    0 = 不需要包装（读语句 / 脚本自己开着事务 / 脚本自己发的是 BEGIN|COMMIT）
    1 = 已开隐式事务，调用方必须在语句结束后调 raft_implicit_finish()
   -1 = **BEGIN 本身被服务端拒了**，调用方必须把这条 BEGIN 的错误报给压测框架
        （原因可能是 1402 / 3197 / 810x —— 实测在 follower 上是 1402 XA_RBROLLBACK）。
        别把它吞成日志再让下一条语句去报错：那样客户端看到的错误类别是错的
        （follower 上会变成 8107 "not leader"，而真正的原因是 BEGIN 的预检失败）。
*/
static int raft_implicit_begin_if_needed(db_mysql_conn_t *c, const char *query, size_t len)
{
  if (c == NULL || !c->managed_identity)
    return 0;

  if (raft_is_txn_start(query, len))
  {
    raft_set_request_id(c);          /* 身份必须在 BEGIN 之前设 */
    c->txn_open = 1;
    return 0;
  }

  if (raft_is_txn_end(query, len))
  {
    c->txn_open = 0;
    return 0;
  }

  if (c->txn_open)                   /* 脚本自己开着事务：不插手 */
    return 0;

  if (!raft_is_write_statement(query, len))
    return 0;                        /* 读语句不受管，也不需要事务 */

  raft_set_request_id(c);

  if (raft_run_sql(c->mysql, "BEGIN") != 0)
  {
    log_text(LOG_WARNING, "Percona Raft: implicit BEGIN failed: %s",
             mysql_error(c->mysql));
    return -1;                       /* 调用方用 check_error() 上报并分类 */
  }

  c->txn_open = 1;
  raft_bump(&raft_implicit_txns);

  return 1;
}

/* 收尾隐式事务：语句成功就 COMMIT，失败就 ROLLBACK（别把残局留给下一条语句） */
static int raft_implicit_finish(db_mysql_conn_t *c, int statement_ok)
{
  int rc;

  if (c == NULL)
    return 0;

  rc = raft_run_sql(c->mysql, statement_ok ? "COMMIT" : "ROLLBACK");
  c->txn_open = 0;

  return rc;                         /* != 0 时 mysql_error(c->mysql) 里有原因 */
}

/* 错误码分栏（按数值；口径见 §3.3 的错误码表） */
static void raft_classify_error(unsigned int error)
{
  switch (error)
  {
    case 3098:   /* ER_BEFORE_DML_VALIDATION_ERROR：执行前校验 */
    case 8109:   /* ER_PERCONA_RAFT_WRITE_UNSUPPORTED */
    case 8111:   /* ER_PERCONA_RAFT_WRITE_SCHEMA_CUTOVER（DDL 排空窗口内的预期拒绝） */
    case 8112:   /* ACL_STATEMENT_FAILED */
    case 8113:   /* ACL_STATEMENT_REJECTED */
    case 8114:   /* METADATA_STATEMENT_REJECTED */
      raft_bump(&raft_cls_refused);
      break;

    case 3197:   /* ER_XA_RETRY：提交结果未知，可带同一 request identity 重试 */
      raft_bump(&raft_cls_ambiguous);
      break;

    case 1402:   /* XA_RBROLLBACK：已决定的回滚 */
      raft_bump(&raft_cls_rolled_back_xa);
      break;

    case 8100: case 8101: case 8103: case 8104:
    case 8105: case 8107: case 8110:
    case 8115:   /* 临时态：换主/多数派抖动/读屏障超时，退避重试可自愈 */
      raft_bump(&raft_cls_temporary);
      break;

    case 8102:   /* NODE_ISOLATED：本节点被隔离，重试不会自愈 */
    case 8106:   /* RECOVERY_REQUIRED */
    case 8108:   /* WRITE_RECOVERY_REQUIRED */
      raft_bump(&raft_cls_needs_action);   /* 需要人工介入，别混进"临时态" */
      break;

    default:
      break;     /* 其它错误不进分类：仍由 sysbench 原有的 errors/FATAL 逻辑处理 */
  }
}

/* Structure used for DB-to-MySQL bind types map */

typedef struct
{
  db_bind_type_t   db_type;
  int              my_type;
} db_mysql_bind_map_t;

/* DB-to-MySQL bind types map */
db_mysql_bind_map_t db_mysql_bind_map[] =
{
  {DB_TYPE_TINYINT,   MYSQL_TYPE_TINY},
  {DB_TYPE_SMALLINT,  MYSQL_TYPE_SHORT},
  {DB_TYPE_INT,       MYSQL_TYPE_LONG},
  {DB_TYPE_BIGINT,    MYSQL_TYPE_LONGLONG},
  {DB_TYPE_FLOAT,     MYSQL_TYPE_FLOAT},
  {DB_TYPE_DOUBLE,    MYSQL_TYPE_DOUBLE},
  {DB_TYPE_DATETIME,  MYSQL_TYPE_DATETIME},
  {DB_TYPE_TIMESTAMP, MYSQL_TYPE_TIMESTAMP},
  {DB_TYPE_CHAR,      MYSQL_TYPE_STRING},
  {DB_TYPE_VARCHAR,   MYSQL_TYPE_VAR_STRING},
  {DB_TYPE_NONE,      0}
};

/* MySQL driver capabilities */

static drv_caps_t mysql_drv_caps =
{
  1,
  0,
  1,
  0,
  0,
  1
};



static mysql_drv_args_t args;          /* driver args */

static char use_ps; /* whether server-side prepared statemens should be used */

/* Positions in the list of hosts/ports/sockets. Protected by pos_mutex */
static sb_list_item_t *hosts_pos;
static sb_list_item_t *ports_pos;
static sb_list_item_t *sockets_pos;

static pthread_mutex_t pos_mutex;

/* MySQL driver operations */

static int mysql_drv_init(void);
static int mysql_drv_thread_init(int);
static int mysql_drv_describe(drv_caps_t *);
static int mysql_drv_connect(db_conn_t *);
static int mysql_drv_reconnect(db_conn_t *);
static int mysql_drv_disconnect(db_conn_t *);
static int mysql_drv_prepare(db_stmt_t *, const char *, size_t);
static int mysql_drv_bind_param(db_stmt_t *, db_bind_t *, size_t);
static int mysql_drv_bind_result(db_stmt_t *, db_bind_t *, size_t);
static db_error_t mysql_drv_execute(db_stmt_t *, db_result_t *);
static int mysql_drv_fetch(db_result_t *);
static int mysql_drv_fetch_row(db_result_t *, db_row_t *);
static db_error_t mysql_drv_query(db_conn_t *, const char *, size_t,
                           db_result_t *);
static int mysql_drv_free_results(db_result_t *);
static int mysql_drv_close(db_stmt_t *);
static int mysql_drv_thread_done(int);
static int mysql_drv_done(void);
static int mysql_drv_report_stats(void);

/* MySQL driver definition */

static db_driver_t mysql_driver =
{
  .sname = "mysql",
  .lname = "MySQL driver",
  .args = mysql_drv_args,
  .ops = {
    .init = mysql_drv_init,
    .thread_init = mysql_drv_thread_init,
    .describe = mysql_drv_describe,
    .connect = mysql_drv_connect,
    .disconnect = mysql_drv_disconnect,
    .reconnect = mysql_drv_reconnect,
    .prepare = mysql_drv_prepare,
    .bind_param = mysql_drv_bind_param,
    .bind_result = mysql_drv_bind_result,
    .execute = mysql_drv_execute,
    .fetch = mysql_drv_fetch,
    .fetch_row = mysql_drv_fetch_row,
    .free_results = mysql_drv_free_results,
    .close = mysql_drv_close,
    .query = mysql_drv_query,
    .thread_done = mysql_drv_thread_done,
    .done = mysql_drv_done,
    .report_stats = mysql_drv_report_stats
  }
};


/* Local functions */

static int get_mysql_bind_type(db_bind_type_t);

/* Register MySQL driver */


int register_driver_mysql(sb_list_t *drivers)
{
  SB_LIST_ADD_TAIL(&mysql_driver.listitem, drivers);

  return 0;
}


/* MySQL driver initialization */


int mysql_drv_init(void)
{
  pthread_mutex_init(&pos_mutex, NULL);

  args.hosts = sb_get_value_list("mysql-host");
  if (SB_LIST_IS_EMPTY(args.hosts))
  {
    log_text(LOG_FATAL, "No MySQL hosts specified, aborting");
    return 1;
  }
  hosts_pos = SB_LIST_ITEM_NEXT(args.hosts);

  args.ports = sb_get_value_list("mysql-port");
  if (SB_LIST_IS_EMPTY(args.ports))
  {
    log_text(LOG_FATAL, "No MySQL ports specified, aborting");
    return 1;
  }
  ports_pos = SB_LIST_ITEM_NEXT(args.ports);

  args.sockets = sb_get_value_list("mysql-socket");
  sockets_pos = args.sockets;

  args.user = sb_get_value_string("mysql-user");
  args.password = sb_get_value_string("mysql-password");
  args.db = sb_get_value_string("mysql-db");
  args.use_ssl = sb_get_value_flag("mysql-ssl");
  args.ssl_cipher = sb_get_value_string("mysql-ssl-cipher");
  args.use_compression = sb_get_value_flag("mysql-compression");
  args.debug = sb_get_value_flag("mysql-debug");
  if (args.debug)
    sb_globals.verbosity = LOG_DEBUG;

  args.ignored_errors = sb_get_value_list("mysql-ignore-errors");

  args.dry_run = sb_get_value_flag("mysql-dry-run");

  use_ps = 0;
  mysql_drv_caps.prepared_statements = 1;
  if (db_globals.ps_mode != DB_PS_MODE_DISABLE)
    use_ps = 1;

  DEBUG("mysql_library_init(%d, %p, %p)", 0, NULL, NULL);
  mysql_library_init(0, NULL, NULL);

  return 0;
}

/* Thread-local driver initialization */

int mysql_drv_thread_init(int thread_id)
{
  (void) thread_id; /* unused */

  const my_bool rc = mysql_thread_init();
  DEBUG("mysql_thread_init() = %d", (int) rc);

  return rc != 0;
}

/* Thread-local driver deinitialization */

int mysql_drv_thread_done(int thread_id)
{
  (void) thread_id; /* unused */

  DEBUG("mysql_thread_end(%s)", "");
  mysql_thread_end();

  return 0;
}

/* Describe database capabilities */

int mysql_drv_describe(drv_caps_t *caps)
{
  *caps = mysql_drv_caps;

  return 0;
}


static int mysql_drv_real_connect(db_mysql_conn_t *db_mysql_con)
{
  MYSQL          *con = db_mysql_con->mysql;
  const char     *ssl_key;
  const char     *ssl_cert;
  const char     *ssl_ca;

  if (args.use_ssl)
  {
    ssl_key= "client-key.pem";
    ssl_cert= "client-cert.pem";
    ssl_ca= "cacert.pem";

    DEBUG("mysql_ssl_set(%p, \"%s\", \"%s\", \"%s\", NULL, \"%s\")", con,
          ssl_key, ssl_cert, ssl_ca, args.ssl_cipher);

    mysql_ssl_set(con, ssl_key, ssl_cert, ssl_ca, NULL, args.ssl_cipher);

#ifdef HAVE_MYSQL_OPT_SSL_MODE
    unsigned int opt_ssl_mode = SSL_MODE_REQUIRED;

    DEBUG("mysql_options(%p, %s, %u)", con, "MYSQL_OPT_SSL_MODE", opt_ssl_mode);
    mysql_options(con, MYSQL_OPT_SSL_MODE, &opt_ssl_mode);
#endif
  }

  if (args.use_compression)
  {
    DEBUG("mysql_options(%p, %s, %s)",con, "MYSQL_OPT_COMPRESS", "NULL");
    mysql_options(con, MYSQL_OPT_COMPRESS, NULL);
  }

  DEBUG("mysql_real_connect(%p, \"%s\", \"%s\", \"%s\", \"%s\", %u, \"%s\", %s)",
        con,
        SAFESTR(db_mysql_con->host),
        SAFESTR(db_mysql_con->user),
        SAFESTR(db_mysql_con->password),
        SAFESTR(db_mysql_con->db),
        db_mysql_con->port,
        SAFESTR(db_mysql_con->socket),
        (MYSQL_VERSION_ID >= 50000) ? "CLIENT_MULTI_STATEMENTS" : "0"
        );

  return mysql_real_connect(con,
                            db_mysql_con->host,
                            db_mysql_con->user,
                            db_mysql_con->password,
                            db_mysql_con->db,
                            db_mysql_con->port,
                            db_mysql_con->socket,
#if MYSQL_VERSION_ID >= 50000
                            CLIENT_MULTI_STATEMENTS
#else
                            0
#endif
                            ) == NULL;
}


/* Connect to MySQL database */


int mysql_drv_connect(db_conn_t *sb_conn)
{
  MYSQL           *con;
  db_mysql_conn_t *db_mysql_con;

  if (args.dry_run)
    return 0;

  db_mysql_con = (db_mysql_conn_t *) calloc(1, sizeof(db_mysql_conn_t));

  if (db_mysql_con == NULL)
    return 1;

  con = (MYSQL *) malloc(sizeof(MYSQL));
  if (con == NULL)
    return 1;

  db_mysql_con->mysql = con;

  DEBUG("mysql_init(%p)", con);
  mysql_init(con);

  pthread_mutex_lock(&pos_mutex);

  if (SB_LIST_IS_EMPTY(args.sockets))
  {
    db_mysql_con->socket = NULL;
    db_mysql_con->host = SB_LIST_ENTRY(hosts_pos, value_t, listitem)->data;
    db_mysql_con->port =
      atoi(SB_LIST_ENTRY(ports_pos, value_t, listitem)->data);

    /*
       Pick the next port in args.ports. If there are no more ports in the list,
       move to the next available host and get the first port again.
    */
    ports_pos = SB_LIST_ITEM_NEXT(ports_pos);
    if (ports_pos == args.ports) {
      hosts_pos = SB_LIST_ITEM_NEXT(hosts_pos);
      if (hosts_pos == args.hosts)
        hosts_pos = SB_LIST_ITEM_NEXT(hosts_pos);

      ports_pos = SB_LIST_ITEM_NEXT(ports_pos);
    }
  }
  else
  {
    db_mysql_con->host = "localhost";

    /*
       The sockets list may be empty. So unlike hosts/ports the loop invariant
       here is that sockets_pos points to the previous one and should be
       advanced before using it, not after.
     */
    sockets_pos = SB_LIST_ITEM_NEXT(sockets_pos);
    if (sockets_pos == args.sockets)
      sockets_pos = SB_LIST_ITEM_NEXT(sockets_pos);

    db_mysql_con->socket = SB_LIST_ENTRY(sockets_pos, value_t, listitem)->data;
  }
  pthread_mutex_unlock(&pos_mutex);

  db_mysql_con->user = args.user;
  db_mysql_con->password = args.password;
  db_mysql_con->db = args.db;

  if (mysql_drv_real_connect(db_mysql_con))
  {
    if (!SB_LIST_IS_EMPTY(args.sockets))
      log_text(LOG_FATAL, "unable to connect to MySQL server on socket '%s', "
               "aborting...", db_mysql_con->socket);
    else
      log_text(LOG_FATAL, "unable to connect to MySQL server on host '%s', "
               "port %u, aborting...",
               db_mysql_con->host, db_mysql_con->port);
    log_text(LOG_FATAL, "error %d: %s", mysql_errno(con),
             mysql_error(con));
    free(db_mysql_con);
    free(con);
    return 1;
  }

  if (args.use_ssl)
  {
    DEBUG("mysql_get_ssl_cipher(con): \"%s\"", mysql_get_ssl_cipher(con));
  }

  sb_conn->ptr = db_mysql_con;

  /* perf-tool fork：探测身份变量并设 namespace（每连接一次） */
  raft_setup_identity(db_mysql_con);

  /*
    --target=raft 的"立刻失败"：连上了、但没有受管写契约。
    必须**返回失败**而不是记条日志继续跑——继续跑会得到一份"unmanaged 增量 0"的漂亮报告，
    而那个 0 只说明"这台根本没有那道门"（§4.0/§7.1），会被读成性能结论。
  */
  if (db_mysql_con->read_consistency_failed)
  {
    log_text(LOG_FATAL, "aborting: --read-consistency could not be applied on "
                        "host '%s', port %u",
             db_mysql_con->host, db_mysql_con->port);
    mysql_close(db_mysql_con->mysql);
    free(db_mysql_con->mysql);
    free(db_mysql_con);
    sb_conn->ptr = NULL;
    return 1;
  }

  if (db_mysql_con->target_mismatch)
  {
    log_text(LOG_FATAL, "aborting: --target=raft requires the managed-write "
                        "contract on host '%s', port %u",
             db_mysql_con->host, db_mysql_con->port);
    mysql_close(db_mysql_con->mysql);
    free(db_mysql_con->mysql);
    free(db_mysql_con);
    sb_conn->ptr = NULL;
    return 1;
  }

  return 0;
}


/* Disconnect from MySQL database */


int mysql_drv_disconnect(db_conn_t *sb_conn)
{
  db_mysql_conn_t *db_mysql_con = sb_conn->ptr;

  if (args.dry_run)
    return 0;
  if (db_mysql_con != NULL && db_mysql_con->mysql != NULL)
  {
    DEBUG("mysql_close(%p)", db_mysql_con->mysql);
    mysql_close(db_mysql_con->mysql);
    free(db_mysql_con->mysql);
    free(db_mysql_con);
  }

  return 0;
}


/* Prepare statement */


int mysql_drv_prepare(db_stmt_t *stmt, const char *query, size_t len)
{
  MYSQL_STMT *mystmt;
  unsigned int rc;

  if (args.dry_run)
    return 0;

  db_mysql_conn_t *db_mysql_con = (db_mysql_conn_t *) stmt->connection->ptr;
  MYSQL      *con = db_mysql_con->mysql;

  if (con == NULL)
    return 1;

  if (use_ps)
  {
    mystmt = mysql_stmt_init(con);
    DEBUG("mysql_stmt_init(%p) = %p", con, mystmt);
    if (mystmt == NULL)
    {
      log_text(LOG_FATAL, "mysql_stmt_init() failed");
      return 1;
    }
    stmt->ptr = (void *)mystmt;
    DEBUG("mysql_stmt_prepare(%p, \"%s\", %u) = %p", mystmt, query,
          (unsigned int) len, stmt->ptr);
    if (mysql_stmt_prepare(mystmt, query, len))
    {
      /* Check if this statement in not supported */
      rc = mysql_errno(con);
      DEBUG("mysql_errno(%p) = %u", con, rc);
      if (rc == ER_UNSUPPORTED_PS)
      {
        log_text(LOG_INFO,
                 "Failed to prepare query \"%s\" (%d: %s), using emulation",
                 query, rc, mysql_error(con));
        goto emulate;
      }
      else
      {
        log_text(LOG_FATAL, "mysql_stmt_prepare() failed");
        log_text(LOG_FATAL, "MySQL error: %d \"%s\"", rc,
                 mysql_error(con));
        DEBUG("mysql_stmt_close(%p)", mystmt);
        mysql_stmt_close(mystmt);
        return 1;
      }
    }

    stmt->query = strdup(query);
    stmt->counter = (mysql_stmt_field_count(mystmt) > 0) ?
      SB_CNT_READ : SB_CNT_WRITE;

    return 0;
  }

 emulate:

  /* Use client-side PS */
  stmt->emulated = 1;
  stmt->query = strdup(query);

  return 0;
}


static void convert_to_mysql_bind(MYSQL_BIND *mybind, db_bind_t *bind)
{
  mybind->buffer_type = get_mysql_bind_type(bind->type);
  mybind->buffer = bind->buffer;
  mybind->buffer_length = bind->max_len;
  mybind->length = bind->data_len;
  /*
    Reuse the buffer passed by the caller to avoid conversions. This is only
    valid if sizeof(char) == sizeof(mybind->is_null[0]). Depending on the
    version of the MySQL client library, the type of MYSQL_BIND::is_null[0] can
    be either my_bool or bool, but sizeof(bool) is not defined by the C
    standard. We assume it to be 1 on most platforms to simplify code and Lua
    API.
  */
#if SIZEOF_BOOL > 1
# error This code assumes sizeof(bool) == 1!
#endif
  mybind->is_null = (my_bool *) bind->is_null;
}


/* Bind parameters for prepared statement */


int mysql_drv_bind_param(db_stmt_t *stmt, db_bind_t *params, size_t len)
{
  MYSQL_BIND   *bind;
  unsigned int i;
  my_bool rc;
  unsigned long param_count;

  if (args.dry_run)
    return 0;

  db_mysql_conn_t *db_mysql_con = (db_mysql_conn_t *) stmt->connection->ptr;
  MYSQL        *con = db_mysql_con->mysql;

  if (con == NULL)
    return 1;

  if (!stmt->emulated)
  {
    if (stmt->ptr == NULL)
      return 1;
    /* Validate parameters count */
    param_count = mysql_stmt_param_count(stmt->ptr);
    DEBUG("mysql_stmt_param_count(%p) = %lu", stmt->ptr, param_count);
    if (param_count != len)
    {
      log_text(LOG_FATAL, "Wrong number of parameters to mysql_stmt_bind_param");
      return 1;
    }
    /* Convert sysbench bind structures to MySQL ones */
    bind = (MYSQL_BIND *)calloc(len, sizeof(MYSQL_BIND));
    if (bind == NULL)
      return 1;
    for (i = 0; i < len; i++)
      convert_to_mysql_bind(&bind[i], &params[i]);

    rc = mysql_stmt_bind_param(stmt->ptr, bind);
    DEBUG("mysql_stmt_bind_param(%p, %p) = %d", stmt->ptr, bind, rc);
    if (rc)
    {
      log_text(LOG_FATAL, "mysql_stmt_bind_param() failed");
      log_text(LOG_FATAL, "MySQL error: %d \"%s\"", mysql_errno(con),
                 mysql_error(con));
      free(bind);
      return 1;
    }
    free(bind);

    return 0;
  }

  /* Use emulation */
  if (stmt->bound_param != NULL)
    free(stmt->bound_param);
  stmt->bound_param = (db_bind_t *)malloc(len * sizeof(db_bind_t));
  if (stmt->bound_param == NULL)
    return 1;
  memcpy(stmt->bound_param, params, len * sizeof(db_bind_t));
  stmt->bound_param_len = len;

  return 0;

}


/* Bind results for prepared statement */


int mysql_drv_bind_result(db_stmt_t *stmt, db_bind_t *params, size_t len)
{
  MYSQL_BIND   *bind;
  unsigned int i;
  my_bool rc;

  if (args.dry_run)
    return 0;

  db_mysql_conn_t *db_mysql_con =(db_mysql_conn_t *) stmt->connection->ptr;
  MYSQL        *con = db_mysql_con->mysql;

  if (con == NULL || stmt->ptr == NULL)
    return 1;

  /* Convert sysbench bind structures to MySQL ones */
  bind = (MYSQL_BIND *)calloc(len, sizeof(MYSQL_BIND));
  if (bind == NULL)
    return 1;
  for (i = 0; i < len; i++)
    convert_to_mysql_bind(&bind[i], &params[i]);

  rc = mysql_stmt_bind_result(stmt->ptr, bind);
  DEBUG("mysql_stmt_bind_result(%p, %p) = %d", stmt->ptr, bind, rc);
  if (rc)
  {
    free(bind);
    return 1;
  }
  free(bind);

  return 0;
}

/* Reset connection to the server by reconnecting with the same parameters. */

static int mysql_drv_reconnect(db_conn_t *sb_con)
{
  db_mysql_conn_t *db_mysql_con = (db_mysql_conn_t *) sb_con->ptr;
  MYSQL *con = db_mysql_con->mysql;

  log_text(LOG_DEBUG, "Reconnecting");

  DEBUG("mysql_close(%p)", con);
  mysql_close(con);

  while (mysql_drv_real_connect(db_mysql_con))
  {
    if (sb_globals.error)
      return DB_ERROR_FATAL;

    usleep(1000);
  }

  log_text(LOG_DEBUG, "Reconnected");

  return DB_ERROR_IGNORABLE;
}


/*
  Check if the error in a given connection should be fatal or ignored according
  to the list of errors in --mysql-ignore-errors.
*/


static db_error_t check_error(db_conn_t *sb_con, const char *func,
                              const char *query, sb_counter_type_t *counter)
{
  sb_list_item_t *pos;
  unsigned int   tmp;
  db_mysql_conn_t *db_mysql_con = (db_mysql_conn_t *) sb_con->ptr;
  MYSQL          *con = db_mysql_con->mysql;

  const unsigned int error = mysql_errno(con);
  DEBUG("mysql_errno(%p) = %u", con, sb_con->sql_errno);

  sb_con->sql_errno = (int) error;

  /* perf-tool fork：错误码分栏（不改原有 ignored errors/FATAL 判定） */
  raft_classify_error(error);

  sb_con->sql_state = mysql_sqlstate(con);
  DEBUG("mysql_state(%p) = %s", con, sb_con->sql_state);

  sb_con->sql_errmsg = mysql_error(con);
  DEBUG("mysql_error(%p) = %s", con, sb_con->sql_errmsg);

  /*
    Check if the error code is specified in --mysql-ignore-errors, and return
    DB_ERROR_IGNORABLE if so, or DB_ERROR_FATAL otherwise
  */
  SB_LIST_FOR_EACH(pos, args.ignored_errors)
  {
    const char *val = SB_LIST_ENTRY(pos, value_t, listitem)->data;

    tmp = (unsigned int) atoi(val);

    if (error == tmp || !strcmp(val, "all"))
    {
      log_text(LOG_DEBUG, "Ignoring error %u %s, ", error, sb_con->sql_errmsg);

      /* Check if we should reconnect */
      switch (error)
      {
      case CR_SERVER_LOST:
      case CR_SERVER_GONE_ERROR:
      case CR_TCP_CONNECTION:
      case CR_SERVER_LOST_EXTENDED:

        *counter = SB_CNT_RECONNECT;

        return mysql_drv_reconnect(sb_con);

      default:

        break;
      }

      *counter = SB_CNT_ERROR;

      return DB_ERROR_IGNORABLE;
    }
  }

  if (query)
    log_text(LOG_FATAL, "%s returned error %u (%s) for query '%s'",
             func, error, sb_con->sql_errmsg, query);
  else
    log_text(LOG_FATAL, "%s returned error %u (%s)",
             func, error, sb_con->sql_errmsg);

  *counter = SB_CNT_ERROR;

  return DB_ERROR_FATAL;
}

/* Execute prepared statement */


db_error_t mysql_drv_execute(db_stmt_t *stmt, db_result_t *rs)
{
  db_conn_t       *con = stmt->connection;
  char            *buf = NULL;
  unsigned int    buflen = 0;
  unsigned int    i, j, vcnt;
  char            need_realloc;
  int             n;

  if (args.dry_run)
    return DB_ERROR_NONE;

  con->sql_errno = 0;
  con->sql_state = NULL;
  con->sql_errmsg = NULL;

  if (!stmt->emulated)
  {
    if (stmt->ptr == NULL)
    {
      log_text(LOG_DEBUG,
               "ERROR: exiting mysql_drv_execute(), uninitialized statement");
      return DB_ERROR_FATAL;
    }

    /* perf-tool fork：预处理语句路径同样要注入身份 / 换 request_id / 自动提交包装 */
    db_mysql_conn_t *db_mysql_con = (db_mysql_conn_t *) con->ptr;
    int implicit = raft_implicit_begin_if_needed(
        db_mysql_con, stmt->query, stmt->query == NULL ? 0 : strlen(stmt->query));

    if (SB_UNLIKELY(implicit < 0))
      return check_error(con, "mysql_stmt_execute(BEGIN)", "BEGIN", &rs->counter);

    int err = mysql_stmt_execute(stmt->ptr);
    DEBUG("mysql_stmt_execute(%p) = %d", stmt->ptr, err);

    if (err)
    {
      if (implicit)
        raft_implicit_finish(db_mysql_con, 0);
      return check_error(con, "mysql_stmt_execute()", stmt->query,
                         &rs->counter);
    }

    if (implicit && raft_implicit_finish(db_mysql_con, 1) != 0)
      return check_error(con, "mysql_stmt_execute(COMMIT)", "COMMIT",
                         &rs->counter);

    if (stmt->counter != SB_CNT_READ)
    {
      rs->nrows = (uint32_t) mysql_stmt_affected_rows(stmt->ptr);
      DEBUG("mysql_stmt_affected_rows(%p) = %u", stmt->ptr,
            (unsigned) rs->nrows);

      rs->counter = (rs->nrows > 0) ? SB_CNT_WRITE : SB_CNT_OTHER;

      return DB_ERROR_NONE;
    }

    err = mysql_stmt_store_result(stmt->ptr);
    DEBUG("mysql_stmt_store_result(%p) = %d", stmt->ptr, err);
    if (err)
    {
      return check_error(con, "mysql_stmt_store_result()", NULL,
                         &rs->counter);
    }

    rs->counter = stmt->counter;
    rs->nrows = (uint32_t) mysql_stmt_num_rows(stmt->ptr);
    DEBUG("mysql_stmt_num_rows(%p) = %u", rs->statement->ptr,
          (unsigned) (rs->nrows));

    return DB_ERROR_NONE;
  }

  /* Use emulation */
  /* Build the actual query string from parameters list */
  need_realloc = 1;
  vcnt = 0;
  for (i = 0, j = 0; stmt->query[i] != '\0'; i++)
  {
  again:
    if (j+1 >= buflen || need_realloc)
    {
      buflen = (buflen > 0) ? buflen * 2 : 256;
      buf = realloc(buf, buflen);
      if (buf == NULL)
      {
        log_text(LOG_DEBUG, "ERROR: exiting mysql_drv_execute(), memory allocation failure");
        return DB_ERROR_FATAL;
      }
      need_realloc = 0;
    }

    if (stmt->query[i] != '?')
    {
      buf[j++] = stmt->query[i];
      continue;
    }

    n = db_print_value(stmt->bound_param + vcnt, buf + j, (int)(buflen - j));
    if (n < 0)
    {
      need_realloc = 1;
      goto again;
    }
    j += (unsigned int)n;
    vcnt++;
  }
  buf[j] = '\0';

  db_error_t rc = mysql_drv_query(con, buf, j, rs);

  free(buf);

  return rc;
}


/* Execute SQL query */


db_error_t mysql_drv_query(db_conn_t *sb_conn, const char *query, size_t len,
                           db_result_t *rs)
{
  db_mysql_conn_t *db_mysql_con;
  MYSQL *con;

  if (args.dry_run)
    return DB_ERROR_NONE;

  sb_conn->sql_errno = 0;
  sb_conn->sql_state = NULL;
  sb_conn->sql_errmsg = NULL;

  db_mysql_con = (db_mysql_conn_t *)sb_conn->ptr;
  con = db_mysql_con->mysql;

  /* perf-tool fork：身份注入 + 显式 BEGIN 时换 request_id + 自动提交包装（见 §7.1） */
  int implicit = raft_implicit_begin_if_needed(db_mysql_con, query, len);

  if (SB_UNLIKELY(implicit < 0))
    return check_error(sb_conn, "mysql_drv_query(BEGIN)", "BEGIN", &rs->counter);

  int err = mysql_real_query(con, query, len);
  DEBUG("mysql_real_query(%p, \"%s\", %zd) = %d", con, query, len, err);

  if (SB_UNLIKELY(err != 0))
  {
    if (implicit)
      raft_implicit_finish(db_mysql_con, 0);
    return check_error(sb_conn, "mysql_drv_query()", query, &rs->counter);
  }

  /*
    语句成了、COMMIT 没成 —— 这笔事务**没有**提交，客户端必须知道：
    报 COMMIT 自己的错误（分类器会把 3197/81xx 记进对应栏）。
  */
  if (implicit && raft_implicit_finish(db_mysql_con, 1) != 0)
    return check_error(sb_conn, "mysql_drv_query(COMMIT)", "COMMIT", &rs->counter);

  /* Store results and get query type */
  MYSQL_RES *res = mysql_store_result(con);
  DEBUG("mysql_store_result(%p) = %p", con, res);

  if (res == NULL)
  {
    if (mysql_errno(con) == 0 && mysql_field_count(con) == 0)
    {
      /* Not a select. Check if it was a DML */
      uint32_t nrows = (uint32_t) mysql_affected_rows(con);
      if (nrows > 0)
      {
        rs->counter = SB_CNT_WRITE;
        rs->nrows = nrows;
      }
      else
        rs->counter = SB_CNT_OTHER;

      return DB_ERROR_NONE;
    }

    return check_error(sb_conn, "mysql_store_result()", NULL, &rs->counter);
  }

  rs->counter = SB_CNT_READ;
  rs->ptr = (void *)res;

  rs->nrows = mysql_num_rows(res);
  DEBUG("mysql_num_rows(%p) = %u", res, (unsigned int) rs->nrows);

  rs->nfields = mysql_num_fields(res);
  DEBUG("mysql_num_fields(%p) = %u", res, (unsigned int) rs->nfields);

  return DB_ERROR_NONE;
}


/* Fetch row from result set of a prepared statement */


int mysql_drv_fetch(db_result_t *rs)
{
  /* NYI */
  (void)rs;  /* unused */

  if (args.dry_run)
    return DB_ERROR_NONE;

  return 1;
}

/* Fetch row from result set of a query */

int mysql_drv_fetch_row(db_result_t *rs, db_row_t *row)
{
  MYSQL_ROW my_row;

  if (args.dry_run)
    return DB_ERROR_NONE;

  my_row = mysql_fetch_row(rs->ptr);
  DEBUG("mysql_fetch_row(%p) = %p", rs->ptr, my_row);

  unsigned long *lengths = mysql_fetch_lengths(rs->ptr);
  DEBUG("mysql_fetch_lengths(%p) = %p", rs->ptr, lengths);

  if (lengths == NULL)
    return DB_ERROR_IGNORABLE;

  for (size_t i = 0; i < rs->nfields; i++)
  {
    row->values[i].len = lengths[i];
    row->values[i].ptr = my_row[i];
  }

  return DB_ERROR_NONE;
}

/* Free result set */

int mysql_drv_free_results(db_result_t *rs)
{
  if (args.dry_run)
    return 0;

  /* Is this a result set of a prepared statement? */
  if (rs->statement != NULL && rs->statement->emulated == 0)
  {
    DEBUG("mysql_stmt_free_result(%p)", rs->statement->ptr);
    mysql_stmt_free_result(rs->statement->ptr);
    rs->ptr = NULL;
  }

  if (rs->ptr != NULL)
  {
    DEBUG("mysql_free_result(%p)", rs->ptr);
    mysql_free_result((MYSQL_RES *)rs->ptr);
    rs->ptr = NULL;
  }

  return 0;
}


/* Close prepared statement */


int mysql_drv_close(db_stmt_t *stmt)
{
  if (args.dry_run)
    return 0;

  if (stmt->query)
  {
    free(stmt->query);
    stmt->query = NULL;
  }

  if (stmt->ptr == NULL)
    return 1;

  int rc = mysql_stmt_close(stmt->ptr);
  DEBUG("mysql_stmt_close(%p) = %d", stmt->ptr, rc);

  stmt->ptr = NULL;

  return rc;
}


/* Uninitialize driver */
/*
  perf-tool fork：附加统计行。**由 db_driver.c 调进原生 "SQL statistics:" 段里**
  （`drv_ops_t.report_stats`；调用点在 `db_report_cumulative()` 的 `reconnects:` 之后），
  所以不是另起一段表——原统计列一个不动，新增的行只是接在它们后面。
  这里只打"契约成立与否 + 错误语义分栏"，不打吞吐/延迟（那是原生那几行的事）。
*/
int mysql_drv_report_stats(void)
{
  static const char *target_name[] = {"auto", "raft", "mysql"};

  log_text(LOG_NOTICE, "    target:                              %s",
           target_name[db_globals.target]);
  log_text(LOG_NOTICE, "    read consistency:                    %s",
           db_globals.read_consistency_explicit
             ? (db_globals.read_linearizable ? "LINEARIZABLE" : "STALE")
             : "default (未干预, 服务端默认 STALE)");
  if (db_globals.read_consistency_explicit)
    log_text(LOG_NOTICE, "    connections that accepted it:        %llu / %llu%s",
             raft_conns_read_consistency, raft_conns_total,
             raft_conns_read_consistency == raft_conns_total
               ? ""
               : "   <-- 有连接没设上：本次不是强读档");
  log_text(LOG_NOTICE, "    request identity injection:          %s",
           raft_identity_available
             ? "on  (server has percona_raft_request_*)"
             : "off (server has no percona_raft_request_*)");
  log_text(LOG_NOTICE, "    connections with injected identity:  %llu / %llu",
           raft_conns_with_identity, raft_conns_total);
  log_text(LOG_NOTICE, "    request ids issued:                  %-6llu",
           raft_request_ids_issued);
  log_text(LOG_NOTICE, "    autocommit txns wrapped by driver:   %-6llu",
           raft_implicit_txns);
  log_text(LOG_NOTICE, "    managed error classes:");
  log_text(LOG_NOTICE, "        refused:                         %-6llu (3098/8109/8111/8112/8113/8114)",
           raft_cls_refused);
  log_text(LOG_NOTICE, "        ambiguous:                       %-6llu (3197 结果未知, 可带同一 identity 重试)",
           raft_cls_ambiguous);
  log_text(LOG_NOTICE, "        temporary:                       %-6llu (8100/8101/8103-8105/8107/8110/8115 可退避重试)",
           raft_cls_temporary);
  log_text(LOG_NOTICE, "        needs_action:                    %-6llu (8102/8106/8108 重试不自愈, 要人工介入)",
           raft_cls_needs_action);
  log_text(LOG_NOTICE, "        rolled_back_xa:                  %-6llu (1402 已决定的回滚)",
           raft_cls_rolled_back_xa);

  return DB_ERROR_NONE;
}

int mysql_drv_done(void)
{
  if (args.dry_run)
    return 0;

  mysql_library_end();

  return 0;
}

/* Map SQL data type to bind_type value in MYSQL_BIND */

int get_mysql_bind_type(db_bind_type_t type)
{
  unsigned int i;

  for (i = 0; db_mysql_bind_map[i].db_type != DB_TYPE_NONE; i++)
    if (db_mysql_bind_map[i].db_type == type)
      return db_mysql_bind_map[i].my_type;

  return -1;
}
