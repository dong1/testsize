
$
$ Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution. 
$
$   This program is free software; you can redistribute it and/or modify 
$   it under the terms of the GNU General Public License as published by 
$   the Free Software Foundation; either version 2 of the License, or
$   (at your option) any later version.
$
$  This program is distributed in the hope that it will be useful, 
$  but WITHOUT ANY WARRANTY; without even the implied warranty of 
$  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the 
$  GNU General Public License for more details. 
$
$  You should have received a copy of the GNU General Public License 
$  along with this program; if not, write to the Free Software 
$  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
$

$
$ $Id$
$
$ utils.msg
$
$ The following set numbers have been assigned:
$
$       1       generic
$       2       backupdb
$       3       copydb
$       4       createdb
$       5       dbstatlink
$       6       deletedb
$       7       renamedb
$       9       master
$       10      optimizedb
$       11      restoredb
$       12      loaddb
$       13      unloaddb
$       14      compactdb
$       15      commdb
$       16      pathdb
$       17      addvoldb
$       18      checkdb
$       19      spacedb
$       20      estimatedb_data
$       21      estimatedb_index
$       22      installdb
$       23      migdb
$       24      diagdb
$       25      lockdb
$       26      killtran
$       27      restartevnt
$       28      prestartldb
$       29      shutdownldb
$       30      mqueueldb
$       31      migrate_v73
$       32      load_audit_trail
$       33      alterdbhost
$       35      repl_server
$       36      repl_agent
$       37      plandump
$       38      paramdump
$       39      changemode
$       40      copylogdb
$       41      appylogdb
$       43      statdump

$set 1 MSGCAT_UTIL_SET_GENERIC
1 无效的长度或字符 `%c' 在数据库名 `%s' 中\n
2 无法打开输出文件 `%s'\n
3 应用工具名丢失或无效\n
4 无效的应用工具名 `%s'\n
5 下面的列表包含了已识别的工具名:\n
6 无效字符 `%c' 在卷名 `%s' 中\n
9 \
%s (cubrid 应用工具) %s\n\
Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.\n
10 \
cubrid 管理工具，版本 %s\n\
用法: %s <工具名> [参数]\n\
输入 '%s <工具名>' 来查看工具的详细帮助.\n\
\n\
可用的管理工具名:\n\
    createdb\n\
    deletedb\n\
    installdb\n\
    renamedb\n\
    copydb\n\
    backupdb\n\
    restoredb\n\
    addvoldb\n\
    spacedb\n\
    lockdb\n\
    killtran\n\
    optimizedb\n\
    compactdb\n\
    diagdb\n\
    emergency_patchlog\n\
    checkdb\n\
    alterdbhost\n\
    plandump\n\
    loaddb\n\
    unloaddb\n\
    paramdump\n\
    statdump\n\
    changemode\n\
    copylogdb\n\
    applylogdb\n\
\n\
%s 是一个 DBMS 工具.\n\
要寻找额外的信息, 可浏览 http://www.cubrid.com\n
12 '%s' 应用工具名是无效的.\n输入 'cubrid --help' 查询帮助.\n
13 '%s' 命令无效.\n输入 'cubrid --help' 查询帮助.\n
14 无法读取 cubrid.conf 中的服务属性.\n
15 @ %s %s: %s\n
16 @ %s %s\n
17 ++ %s '%s' 没有运行.\n
18 ++ %s 没有运行.\n
19 ++ %s '%s' 正在运行.\n
20 ++ %s 正在运行.\n
21 ++ %s %s: %s\n
22 缺少一个或多个参数.\n输入 'cubrid --help' 查询帮助.\n
23 cubrid 应用工具, 版本 %s\n\
用法: %s <工具名> [参数]\n\
输入 '%s <工具名>' 来查看工具的详细帮助.\n\
\n\
可用的服务工具:\n\
    service\n\
    server\n\
    broker\n\
    manager\n\
    repl_server\n\
    repl_agent\n\
    heartbeat\n\
\n\
可用的管理员工具:\n\
    addvoldb\n\
    alterdbhost\n\
    backupdb\n\
    checkdb\n\
    compactdb\n\
    copydb\n\
    createdb\n\
    deletedb\n\
    diagdb\n\
    emergency_patchlog\n\
    installdb\n\
    killtran\n\
    loaddb\n\
    lockdb\n\
    optimizedb\n\
    plandump\n\
    renamedb\n\
    restoredb\n\
    spacedb\n\
    unloaddb\n\
    paramdump\n\
    statdump\n\
    changemode\n\
    copylogdb\n\
    applylogdb\n\
\n\
%s 是一个 DBMS 工具.\n\
要寻找额外的信息, 可浏览 http://www.cubrid.com\n
24 cubrid 应用工具, 版本 %s\n\
用法: %s service <命令>\n\
\n\
可用命令:\n\
    start\n\
    stop\n\
    restart\n\
    status\n
25 cubrid 应用工具, 版本 %s\n\
用法: %s server <命令> [参数]\n\
\n\
可用命令:\n\
    start       [数据库名]\n\
    stop        [数据库名]\n\
    restart     [数据库名]\n\
    status\n
26 cubrid 应用工具, 版本 %s\n\
用法: %s broker <命令> [参数]\n\
\n\
可用命令:\n\
    start\n\
    stop\n\
    restart\n\
    status      [-b] [-f] [-q] [-t] [-s <sec>] [<expr>]\n\
    on          broker-name\n\
    off         broker-name\n
27 cubrid 应用工具, 版本 %s\n\
用法: %s manager <命令>\n\
\n\
可用命令:\n\
    start\n\
    stop\n\
    status\n
28 cubrid 应用工具, 版本 %s\n\
用法: %s repl_server <命令> [参数]\n\
\n\
可用命令:\n\
    start       主数据库名 服务端网络端口 [-a 最大 agent 数] [-e 错误文件]\n\
    stop        主数据库名\n\
    status\n
29 cubrid 应用工具, 版本 %s\n\
用法: %s repl_agent <命令> [参数]\n\
\n\
可用命令:\n\
    start       dist-database-name [dba-password]\n\
    stop        dist-database-name\n\
    status\n
30 cubrid 应用工具, 版本 %s\n\
用法: %s heartbeat <命令> [参数]\n\
\n\
可用命令:\n\
    act\n\
    deact\n\
    deregister  process-id\n\
    list\n\
    reload\n
31 不需要参数 '%s' .\n
32 缺少数据库名.\n
33 当前平台不支持 Replication.

$set 2 MSGCAT_UTIL_SET_BACKUPDB
30 警告: 在单机模式下 -t 选项将被忽略.\n
60 \
backupdb: 备份一个数据库.\n\
usage: %s backupdb [选项] 数据库名\n\
\n\
有效选项:\n\
  -D, --destination-path=PATH  将备份卷存到 PATH 目录; 目录的默认值: 数据库的日志目录\n\
  -r, --remove-archive         删除不必要的日志档案; 请参考说明小心使用\n\
  -l, --level=LEVEL            备份级别; 默认: full level\n\
                               允许的级别:\n\
                                   0 - full\n\
                                   1 - incremental 1\n\
                                   2 - incremental 2\n\
  -o, --output-file=FILE       将备份的细节信息打印到文件 FILE\n\
  -S, --SA-mode                单机模式执行\n\
  -C, --CS-mode                客户端-服务器模式执行\n\
      --no-check               不检测数据库的一致性; 默认: 检测\n\
  -t, --thread-count=COUNT     线程数; 默认: 0(auto)\n\
  -z, --compress               压缩备份卷; 默认: 不压缩\n\
  -e, --except-active-log      不包括活动日志卷; 默认: 包括\n
71 不兼容的参数, \"%s\", 和 \"%s\"\n


$set 3 MSGCAT_UTIL_SET_COPYDB
30 错误: 目的名与源名相同.\n
31 错误: volext-pathname不存在或不可写.\n
32 错误: vols-tofrom-paths_file不存在或不可读.\n
60 \
copydb: 复制一个数据库.\n\
用法: %s copydb [选项] 源数据库名 目的数据库名\n\
\n\
有效选项:\n\
      --server-name=HOST          指定服务器的主机名为HOST; 默认: 当前主机名\n\
  -F, --file-path=PATH            指定数据库的卷路径为 PATH; 默认: 当前目录\n\
  -L, --log-path=PATH             指定日志卷的路径为 PATH; 默认: 等于file-path\n\
  -E, --extended-volume-path=PATH 指定 PATH 作为复制/重命名 扩展卷的目录; 默认: 等于原始卷的路径或给定的control-file路径\n\
  -i, --control-file=FILE         FILE 用来控制 复制/重命名 在多个位置上的卷\n\
  -r, --replace                   替换一个存在的数据库; 默认: 不替换\n\
  -d, --delete-source             删除一个源数据库; 默认: 不删除\n


$set 4 MSGCAT_UTIL_SET_CREATEDB
40 组 `%s' 不存在\n
41 用户 `%s' 不存在\n
42 未知的命令 `%s'\n
43 无法打开输出文件 `%s'\n
44 无法移除已存在的数据库.\n
45 创建一个 %d 页的数据库.\n
46 无法创建数据库.\n
47 用户文件 `%s' 存在问题\n
48 页的数目必须大于等于 100\n
49 数据库 `%s' 被成功的创建在 `%s'.\n如果你想运行数据库, 执行命令 `start_server %s'.\nI如果你想执行查询, 执行命令 `sqlx %s'.\n
60 \
createdb: 创建一个数据库.\n\
用法: %s createdb [选项] 数据库名\n\
\n\
有效选项:\n\
  -p, --pages=NUMBER                  设定第一个卷的页数目为NUMBER; 默认: 5000\n\
      --comment=COMMENT               用COMMENT来注释一个数据库; 默认: 空\n\
  -F, --file-path=PATH                用PATH作为数据库的卷目录; 默认: 当前目录\n\
  -L, --log-path=PATH                 用PATH作为日志卷的目录; 默认: 与file-path相同\n\
      --server-name=HOST              用HOST作为服务器的主机名; 默认: 当前主机名\n\
  -r, --replace                       替换一个已存在的数据库; 默认: 不替换\n\
      --more-volume-file=FILE         使用 FILE 中用于创建附加卷的详述; 默认: 无\n\
      --user-definition-file=FILE     使用 FILE 中包含的关于用户的定义; 默认: 无\n\
      --csql-initialization-file=FILE 使用 FILE 中包含的用于初始化数据库的SQls; 默认: 无\n\
  -o, --output-file=FILE              重定向输出信息到FILE; 默认: 无\n\
  -v, --verbose                       使用详细状态信息; 默认: 不可用\n\
  -l, --log-page-count=NUMBER         用 NUMBER 作为日志档案的页数目; 默认: 和第一类卷相同\n\
  -s, --page-size=SIZE                设置数据库的页大小为SIZE字节; 默认: 4KB\n\
      --log-page-size=SIZE            设置日志页的大小为SIZE字节; 默认: 数据库的页大小\n


$set 6 MSGCAT_UTIL_SET_DELETEDB
60 \
deletedb: 删除一个数据库.\n\
用法: %s deletedb [选项] 数据库名\n\
\n\
有效选项:\n\
  -o, --output-file=FILE       重定向输出信息到 FILE; 默认: 无\n\
  -d, --delete-backup          删除备份卷; 默认: 不删除\n

$set 7 MSGCAT_UTIL_SET_RENAMEDB
60 \
renamedb: 重命名数据库.\n\
usage: %s renamedb [选项] 源数据库名 目的数据库名\n\
\n\
有效的选项:\n\
  -E, --extended-volume-path=PATH 指定 PATH 作为复制/重命名 扩展卷的目录; 默认: 等于原始卷的路径或给定的控制文件路径\n\
  -i, --control-file=FILE         FILE 用来控制 复制/重命名 在多个位置上的卷\n\
  -d, --delete-backup             删除备份卷; 默认: 不删除\n

$set 9 MSGCAT_UTIL_SET_MASTER
11 一个 %s 进程 已经在运行.\n
12 Matser服务运行. \n
13 Master服务退出.\n
14 查找本地主机名错误\n
15 无法找到参数文件\n
16 无法初始化主进程\n
17 服务 %s 在 %d 分钟后停止
18 服务 %s 被通知要关闭.\n这可能会占用几分钟. 请等待.
19 服务 %s 没有找到.
20 Master将在 %d 分钟内停止\n
21 repl_server %s 被通知要关闭.\n这可能会占用几分钟. 请等待.
22 repl_agent %s 被通知要关闭.\n这可能会占用几分钟. 请等待.


$set 10 MSGCAT_UTIL_SET_OPTIMIZEDB
11 无法打开输出文件 `%s'\n
60 \
optimizedb: 优化一个数据库.\n\
usage: %s optimizedb [选项] 数据库名\n\
\n\
有效选项:\n\
  -n, --class-name=NAME        更新名为NAME的类的统计信息; 默认: 所有类\n


$set 11 MSGCAT_UTIL_SET_RESTOREDB
19 无效的日期格式, 有效地日期格式是 dd-mm-yyyy:hh:mm:ss or 'backuptime'\n
20 Restoredb 被取消或发生了一个错误.\n
60 \
restoredb: 恢复一个数据库.\n\
usage: %s restoredb [选项] 数据库名\n\
\n\
有效选项:\n\
  -d, --up-to-date=DATE             将数据库恢复到给定日期DATE时的情况. DATE 为如下形式: dd-mm-yyyy:hh:mm:ss 或 'backuptime' 关键字\n\
      --list                        列出不恢复的卷n\
  -B, --backup-file-path=PATH       PATH 备份卷被保存的目录\n\
  -l, --level=LEVEL                 指定将要被恢复的备份的等级为 LEVEL ; 查看 backup 的使用说明; 默认: 0(full backup)\n\
  -p, --partial-recovery            执行部分恢复，如果有任何日志档案存在\n\
  -o, --output-file=FILE            将输出信息重定向到FILE; 默认: 空\n\
  -u, --use-database-location-path  将数据库和日志文件恢复到数据库位置文件中指定的路径中去\n


$set 12 MSGCAT_UTIL_SET_LOADDB
19 数据库名丢失.\n
20 '%s' 文件没有 %d 行.\n
21 中断 ... \n
22 中断读取 - 当前事务被提交\n
23 中断读取 - 当前事务被终止\n
24 无法打开文件 `%s'\n
25 \n*** 语法检测 ***\n\n
26 %d 个错误被检测到\n
27 \n*** 插入对象 ***\n\n
28 总共 %8d 个对象被插入.\n
29 总共 %8d 个被涉及的对象以默认内容被插入.\n
30 \n*** 提交事务中 ***\n
31 \n*** 关闭数据库 ***\n
32 行 %d:
33 解析错误 `%s' 作为一个 `%s' 值 关于属性 `%s' of `%s'\n
34 缺少域关于属性 %s of %s\n
35 对于属性 %s of %s, 类型 %s 在域的集合中是不被允许的\n
36 对于属性 %s of %s, 期望类型 %s, 得到一个set常量\n
37 对于属性 %s of %s, 期望类型 %s, 得到 %s\n
38 没有名为 %s 的类被属性 %s of %s 所引用\n
39 没有名为 %s 的类, 忽略元组\n
40 没有以 %d 为表示的类被属性 %s of %s 所引用\n
41 在加载器输入文件中类 %s 不被允许\n
42 以错误而终止.\n
43 警告: 更新已存在的实例 !\n
44 警告: 重定义实例 %d 属于类 %s\n
45 实例 %d 属于类 %s 已经被定义\n
46 实例 %d 属于类 %s 已经被保留\n
47 在属性 `%s' 上违反了唯一性, 转化为 NULL\n
48   %d 个实例\n
49 类 %s\n
50 密码:
51 \n*** 更新类统计信息 ***\n
52 %s 在行 %d 附近(文本是 '%s')\n
53 词法错误
54 行 %d: 语法错误， 在 \"%s\"
55 丢失
56 在 %s
57 不兼容的参数, \"%s\", 和 \"%s\"\n
58 %d 个实例被提交\n
59 \n无法调用非优化版本的loaddb.\n
60 处理错误 \"%s\", 作为 %s 类型.\n
112   %d 个实例\r
113 最后提交的行 : %d
114 错误控制文件
115 input file to control error(s) during loading
120 \
loaddb: 将对象和结构导入到数据库.\n\
usage: %s loaddb [选项] 数据库名\n\
\n\
有效选项:\n\
  -u, --user=ID                 用于访问数据库的用户ID; 默认: "public"\n\
  -p, --password=PASS           用户密码; 默认: 空\n\
      --data-file-check-only    对数据文件进行语法检测而不加载; 默认: 检测之后加载\n\
  -l, --load-only               加载数据文件而不检测语法; 默认: 检测之后加载\n\
      --estimated-size=NUMBER   用NUMBER作为估计的实例数目; 默认: 5000\n\
  -v, --verbose                 开启详细状态信息; 默认: 关闭\n\
  -c, --periodic-commit=COUNT   每插入COUNT个实例进行周期性的提交; 默认: 在读取时不提交\n\
      --no-oid                  不使用 OID\n\
      --no-statistics           在读取时不更新统计信息\n\
  -s, --schema-file=FILE[:LINE] 从文件 FILE 的第 LINE 行创建结构(schema)\n\
  -i, --index-file=FILE[:LINE]  从文件 FILE 的第 LINE 行创建索引\n\
  -d, --data-file=FILE          从文件 FILE 读取数据\n\
      --error-control-file=FILE 指定文件 FILE 用来描述在读取数据过程中如何处理特定的错误\n\
      --ignore-class-file=FILE  指定文件 FILE 用来描述要忽略掉的类\n
121 \
load_noopt: 不赞成使用这个工具. 小心使用.\n\
usage: load_noopt [选项] 数据库名\n\
\n\
有效选项:\n\
  -u, --user=ID                 同来访问数据库的用户ID; 默认: "public"\n\
  -p, --password=PASS           用户密码; 默认: 空\n\
      --data-file-check-only    对数据文件进行语法检测而不加载; 默认: 检测后读取\n\
  -l, --load-only               读取数据而不进行语法检测; 默认: 检测后读取\n\
      --estimated-size=NUMBER   用NUMBER作为估计的实例数目; 默认: 5000\n\
  -v, --verbose                 启用详细状态信息; 默认: 不可用\n\
  -c, --periodic-commit=COUNT   当插入COUNT个实例进行周期性的提交; 默认: 读取是不提交\n\
      --no-oid                  不使用OID\n\
  -s, --schema-file=FILE[:LINE] 从文件 FILE 的第 LINE 行创建结构(schema)\n\
  -i, --index-file=FILE[:LINE]  从文件 FILE 的第 LINE 行创建索引\n\
  -d, --data-file=FILE          从文件 FILE 读取数据\n\
      --error-control-file=FILE 指定文件 FILE 用来描述在读取数据过程中如何处理特定的错误\n


$set 13 MSGCAT_UTIL_SET_UNLOADDB
41 缓存页数值无效.\n
42 缓存页大小无效.\n
43 %d 个对象被转储.\n
44 类 %s
45 , %d 个实例.\n
46 只有 %d 个对象在 %d 个对象之中被成功转储.\n
47 输出目录过长.\n
48 LSA : %d %d \n
60 \
unloaddb: 从数据库导出对象和结构.\n\
usage: %s unloaddb [选项] 数据库名\n\
\n\
有效选项:\n\
  -i, --input-class-file=FILE 指定包含表名的输入文件; 默认: 转储所有类\n\
      --include-reference     和-i选项一起使用.当卸载了指定数据库类，卸载对象的引用; 默认: disabled\n\
      --input-class-only      和-i选项一起使用. 只创建与输入文件中的表相关的方案(schema)文件; 默认: 未启用\n\
      --lo-count=COUNT        指明一个目录中要被创建的大对象(large object)文件的数目; 默认: 0\n\
      --estimated-size=NUMBER 指定预期的实例数目; 默认: 自动计算\n\
      --cached-pages=NUMBER   配置要被缓存进内存的对象数目; 默认: 100\n\
  -O, --output-path=PATH      用 PATH 作为输出目录; 默认: 工作目录\n\
  -s, --schema-only           只转储结构(schema); 默认: 结构和对象\n\
  -d, --data-only             只转储对象; 默认: 结构和对象\n\
      --output-prefix=PREFIEX 指定输出文件的前缀; 默认: 数据库名\n\
      --hash-file=FILE        指定hash文件名称; 默认: 由 tmpnam() 生成\n\
  -v, --verbose               开启详细状态信息; 默认: 不可用\n\
      --use-delimiter         用 '"' 作为一个标示符的开始和结束; 默认: 不使用\n\
  -S, --SA-mode               单机模式执行\n\
  -C, --CS-mode               客户端-服务器模式执行\n


$set 14 MSGCAT_UTIL_SET_COMPACTDB
11 \n传递 1\n\n
12 %d 个对象被处理.\n
13 \n传递 2\n\n
14 类 %s\n
15 oid:  %d|%d|%d,\n
16 %d 个对象.\n
17 更新对象.\n
18 ref_oid:  %d.%d.%d, ref_class_oid:  %d.%d.%d\n
19 无法转换对象\n
20 类的对文件缺失\n
21 无法更新对象\n
22 只有 %d 个对象在 %d 个对象之中被成功处理.\n
23 COMPACTDB 已经启动.\n
24 页的数目必须在 %d 和 %d 之间.\n
25 实例锁的超时时间必须在 %d 和 %d 之间.\n
26 对象总数: %d\n
27 失败的对象: %d\n
28 修改的对象: %d\n
29 大对象: %d\n
30 旧的类表述从目录中删除.\n
31 旧的类表述在目录中保留.\n
32 无法设置隔离级别.\n
33 无法压缩数据库.\n
34 类锁的超时必须在 %d 和 %d 之间.\n
35 类被锁住... 不完全的处理...\n
36 未处理的类...\n
37 处理错误... 不完全的处理...\n
38 没有什么可被处理...\n
39 无效的参数...\n
40 未知的类名...\n
41 未使用的类OID和页已被再生.\n
42 OID和页的再生已经被跳过.\n
43 在处理类时出错, 类被跳过.\n
44 \n传递 3\n\n
45 堆压缩失败...\n
46 堆压缩成功...\n
60 \
compactdb: 释放数据库中未使用的空间.\n\
usage: %s compactdb [选项] 数据库名 [类名1, 类名2,...]\n\
\n\
有效选项:\n\
  -v, --verbose                     开启详细状态信息; 默认: 不可用\n\
  -i, --input-class-file=FILE       类名的输入文件\n\
                                    class_name 列表必须为空\n\
                                    默认: 压缩所有的类\n\
                                    只在CS模式中有效\n\
  -p, --pages-commited-once=NUMBER  一次提交的最大页数目\n\
                                    默认: 10\n\
                                    只在CS模式中可用\n\
  -S, --SA-mode                     单机模式执行\n\
  -C, --CS-mode                     客户端-服务端模式执行\n\
  -d, --delete-old-repr             从目录中删除旧的类表述; 默认: 不使用\n\
  class_name_list                   只在CS模式中有效\n\
  -I, --Instance-lock-timeout       对于实例锁超时\n\
  -c, --class-lock-timeout          对于类锁超时\n

$set 15 MSGCAT_UTIL_SET_COMMDB
21 CUBRID 主进程运行在主机 %s \n被启动在 %s
22 收到了 %d 请求. %d 个服务在运行.\n
23 在 %s 分钟内关闭\n
24 %s
25 要关闭的服务名 :
26 多少分钟后关闭? :
27 \
cub_commdb: 这个工具不赞成使用. 用 'cubrid' 命令来代替.\n\
usage: cub_commdb [选项]\n\
\n\
有效选项:\n\
  -P, --server-list                打印服务进程\n\
  -R, --repl-list                  打印复制(Replication)进程\n\
  -O, --all-list                   打印所有进程\n\
  -S, --shutdown-server=NAME       关闭名为 NAME 的数据库服务\n\
  -K, --shutdown-repl-server=NAME  关闭名为 NAME 的复制(Replication)服务\n\
  -k, --shutdown-repl-agent=NAME   关闭名为 NAME 的复制(Replication)代理\n\
  -A, --shutdown-all               关闭所有进程\n\
  -c, --server-mode=NAME           打印名为 NAME 的数据库的HA 模式\n\
  -N, --node-list                  列出由 CUBRID heartbeat 所管理的结点\n\
  -L, --process-list               列出由 CUBRID heartbeat 所管理的本地进程\n\
  -D, --dereg-process=PID          退出目标进程并从被管理的进程条目中将其移除\n\
  -F, --reconfig-node-list         应用最近改变的 CUBRID heartbeat 配置\n\
  -U, --deactivate-heartbeat       撤销 CUBRID heartbeat 特征并从节点组中移除当前节点\n\
  -T, --activate-heartbeat         激活CUBRID heartbeat 特征并将当前节点加入到节点组中\n\
  -V, --verbose                    开启详细状态信息; 默认: 不可用\n
28 对不起，未知的命令: %s\n
29 无法得到主机名或本地主机 - 终止\n
30 找不到参数文件\n
31 无法在 %s 连接主服务\n
32 多少分钟后关闭? :
33 主进程(Master) 将在 %d 分钟内关闭\n
34 从客户端那里收到了错误的数据
35 数据库服务 %s 当前没有运行
36 无法执行 shell (%s) 关于 %s, %s
37 进程 fork %s, 返回 %d, %s
38 无法执行 %s


$set 16 MSGCAT_UTIL_SET_PATCHDB
60 \
emergency_patchlog: 紧急情况下修补一个数据库.\n\
usage: %s emergency_patchlog [选项] 数据库名\n\
\n\
有效选项:\n\
  -r, --recreate-log           重建日志文件; 小心使用\n


$set 17 MSGCAT_UTIL_SET_ADDVOLDB
20 错误的值 %d 对于 页的数目\n
21 错误的值 '%s' 对于卷存储目的.\n
60 \
addvoldb: 为数据库添加一个卷文件.\n\
usage: %s addvoldb [选项] 数据库名 页的数目\n\
\n\
有效选项:\n\
  -n, --volume-name=NAME       信息卷的名称; 默认: 像这样生成 "db"_ext1)\n\
  -F, --file-path=PATH         为将被添加的卷文件指定一个创建目录; 默认: 工作目录\n\
      --comment=COMMENT        给添加的卷文件加注释; 默认: 无\n\
  -p, --purpose=PURPOSE        指明添加卷文件的目的; 允许的值:\n\
                                       DATA - 为了数据\n\
                                       INDEX - 为了索引\n\
                                       TEMP - 临时的\n\
                                       GENERIC - 为了所有的\n\
  -S, --SA-mode                单机模式执行\n\
  -C, --CS-mode                客户端-服务器模式执行\n

$set 18 MSGCAT_UTIL_SET_CHECKDB
20 一些不一致性在你的数据库中被检测出来.\n 请在错误日志文件 %s 中查阅额外的信息.\n
60 \
checkdb: 监测数据库一致性.\n\
usage: %s checkdb [选项] 数据库名\n\
\n\
有效选项:\n\
  -S, --SA-mode                单机模式执行\n\
  -C, --CS-mode                客户端-服务器模式执行\n\
  -r, --repair                 修复找到的不一致性\n


$set 19 MSGCAT_UTIL_SET_SPACEDB
15 数据库 '%s' 的空间描述 页大小 %d. (日志页大小: %d)\n\n
16 %5d %8s  %s %s  %s\n
17 无法打开输出文件 `%s'\n
18 数据库 '%s' 临时卷的空间描述 页大小 %d.\n\n
19 卷标识  用途  页总数  空闲页  卷名\n\n
20 卷标识  用途  总大小  空闲空间  卷名\n\n
60 \
spacedb: 显示数据库的使用/空闲空间.\n\
usage: %s spacedb [选项] 数据库名\n\
\n\
有效选项:\n\
  -o, --output-file=FILE       重定向输出信息到文件 FILE; 默认: 无\n\
  -S, --SA-mode                单机模式执行\n\
  -C, --CS-mode                客户端服务端模式执行\n\
      --size-unit={PAGE|M|G|T|H}\n\
                               设置显示单元 (页, M, G, T, 人类可读);默认 : 页\n


$set 20 MSGCAT_UTIL_SET_ESTIMATEDB_DATA
15 预计页的数目 = %d\n
60 \
估算数据: 估算数据的大小.\n\
用法: %s 估算数据 实例的数目 实例的平均大小 属性的数目 可变属性数目\n


$set 21 MSGCAT_UTIL_SET_ESTIMATEDB_INDEX
15 指定了不正确的索引键类型 = %s\n
16 指定了不正确的平均键长度 = %d when precision = %d\n
17 指定了错误的参数\n
20 估算的全部索引页数目 = %d\n (在实例被写入后，索引被创建.)\n
21 估算的全部索引页数目 = %d\n (在实例被写入前(平均情况)，索引被创建)\n
22 估算的全部索引页数目 = %d\n (在实例被写入前(最坏情况)，索引被创建)\n
23 用于给定的输入...\n
24 实例的总数               :%d\n
25 不同键的数目             : %d\n
26 键的平均大小             : %d\n
27 键的类型                 : %s\n\n
60 \
估算索引: 估算一个索引的大小.\n\
用法: %s 估算索引 实例的数目 不同键的数目 平均键长 键类型\n

$set 22 MSGCAT_UTIL_SET_INSTALLDB
60 \
installdb: 安装一个数据库.\n\
usage: %s installdb [选项] 数据库名\n\
\n\
有效选项:\n\
      --server-name=HOST       指定服务端的主机名; 默认: 当前主机名e\n\
  -F, --file-path=PATH         注册数据库的卷目录 PATH 到databases.txt; 默认: 当前目录\n\
  -L, --log-path=PATH          注册数据库的日志卷目录 PATH 到databases.txt; 默认: 和 file-path 相同\n


$set 23 MSGCAT_UTIL_SET_MIGDB
1 临时类OID\n
2 无法打印 elo 数值\n
3 访问用于迁移的长对象错误\n
4 打开用于迁移长对象的输出文件错误\n
5 读错误\n
6 写错误\n
7 打开ELO输入文件错误 "%s"\n
8 类:
9 哈希文件
10 名称:                         %s\n
11 大小:                         %d\n
12 页大小:                       %d\n
13 数据大小:                     %d\n
14 条目大小:                     %d\n
15 每一页的条目:                 %d\n
16 缓存的页:                     %d\n
17 条目的数目:                   %d\n
18 冲突的数目:                   %d\n
19 哈希文件名:                   %s\n
20 下一个溢出条目:               %d\n
21 键的类型:                     %s\n
22 页头的地址:                   %x\n
23 最后的页头地址:               %x\n
24 空闲页的头地址:               %x\n
25 页位图的地址:                 %x\n
26 页位图的大小:                 %d\n


$set 24 MSGCAT_UTIL_SET_DIAGDB
60 \
diagdb: 转储一个数据库.\n\
usage: %s diagdb [选项] 数据库名\n\
\n\
有效选项:\n\
  -d, --dump-type=TYPE         转储类型\n\
                               -1 转储所有\n\
                                1 转出文件表\n\
                                2 转储文件容量\n\
                                3 转储堆容量\n\
                                4 转储索引容量\n\
                                5 转储类名\n\
                                6 转储磁盘位图\n\
                                7 转储目录\n\
                                8 转出日志\n\
                                9 转储堆\n


$set 25 MSGCAT_UTIL_SET_LOCKDB
15 无法打开输出文件 `%s'\n
59 lockdb 无法在单机模式下运行.\n
60 \
lockdb: 显示数据库的锁状态.\n\
usage: %s lockdb [选项] 数据库名\n\
\n\
有效选项:\n\
  -o, --output-file=FILE       重定向输出信息到文件 FILE ; 默认: 空\n


$set 26 MSGCAT_UTIL_SET_KILLTRAN
20 在事务索引，用户名，主机，程序名之间，只能指定一个参数\n
21 输入 DBA 密码:
22 事务索引        用户名         主机名         进程 id         程序名   \n
23 -------------------------------------------------------------------------------\n
24 %7d(%1c) %14s %14s %15d %17s\n
25 没有事务\n
26 没有一个事务带有指定的参数值.\n
27 准备取消下面的事务:\n\n
28 你希望进行吗 ? (Y/N)
29 取消与事务索引 %d相关联的事务\n
30 取消如下事务失败\n\n
31 事务目前还没有终止，但是可能在做一些回滚工作.\n 请稍等片刻，如果有必要，可以稍后再做尝试.\n
59 无法在单机模式下取消事务.\n
60 \
killtran: 管理数据库的事务.\n\
usage: %s killtran [选项] 数据库名\n\
\n\
有效选项:\n\
  -i, --kill-transaction-index=INDEX 通过事务索引取消事务\n\
      --kill-user-name=ID            通过用户ID取消所有事务\n\
      --kill-host-name=HOST          通过客户端主机名取消所有事务\n\
      --kill-program-name=NAME       通过客户端程序名取消所有事务\n\
  -p, --dba-password=PASS            DBA用户的密码; 如果没有指定将提示输入\n\
  -d, --display-information          显示关于活动事务的信息\n\
  -f, --force                        取消事务而不需要核对提示\n


$set 31 MSGCAT_UTIL_SET_MIGRATE
1 数据库名
2 需要参数, 没有默认值
3 兼容性检测
4 只进行兼容性检测
5 备份目的地
6 一个用于备份卷目文件的目录或设备
20 数据库 '%s' 不存在.
21 '%s' 无法被迁移到 %s, 因为数据库的版本太低.\n
22 '%s' 不需要被迁移.\n
23 '%s' 需要被迁移.\n
24 推荐在迁移后进行备份. 你希望现在就备份数据库吗? (y/n):
25 我们推荐在迁移后用backupdb命令备份你的新数据库.
26 \n*** 警告: 在迁移到 %s 前备份你的数据库***\n\n为了恢复可能在迁移中产生的错误，我们强烈推荐备份你的数据库.\n你已经备份了你的数据库吗? (y/n):
27 \n1. 运行 backupdb \n2. 为新的数据库输入新的备份路径.(当前路径是: %s)\n3. 退出 \n
28 \n数据库的新备份路径:
29 \n迁移和备份成功的完成.
30 备份失败. 请再次尝试备份数据库.
31 \n\n*** 警告 : 一旦迁移完成, 您不能用以前的备份卷来恢复数据.\n注意，只有在restoredb版本正确的情况下，之前版本的备份文件才可以被恢复.\n请确保之前的备份卷被安全的保存.\n

$set 33 MSGCAT_UTIL_SET_ALTERDBHOST
60 \
alterdbhost: 改变数据库的主机.\n\
用法: %s alterdbhost [选项] 数据库名\n\
\n\
有效选项:\n\
  -h, --host=HOST              指定HOST为新的主机; 默认: 当前主机名\n


$set 34 MSGCAT_UTIL_SET_LOADJAVA
1 数据库名
2 需要参数, no default
3 java类文件
4 需要参数, no default
5 强制重写
6 如果类存在则进行重写, 默认 不

$set 35 MSGCAT_UTIL_SET_REPLSERVER
1 数据库名
2 端口号
3 错误日志文件
4 代理的数目
11 内部错误(%d): 通信错误
12 内部错误(%d): 无法申请内存
13 内部错误(%d): IO 错误
14 内部错误(%d): 通用
21 错误(%d): 无法打开master的活动日志文件 - %s.
22 错误(%d): 无法打开databases.txt文件.
23 错误(%d): %s 在databases.txt中不不存在.
24 错误(%d): 无法打开目录文件.
25 错误(%d): 无效的参数. 请提供正确的参数.
26 错误(%d): 无法连接到master. 请检查repl_server是否在运行, 或master是否在运行.
27 错误(%d): repl_server的端口号已经被使用. 请检查使用相同TCP端口的repl_server是否已经在运行.
28 警告(%d): 超过最大代理连接数.忽略客户端连接请求.
29 警告(%d): 关闭repl_agent的连接.
30 警告(%d): 请求队列已满. 忽略请求.
31 错误(%d): 无法打开日志归档日志 - %s.
32 错误(%d): 无法读取日志归档日志 - %s.

$set 36 MSGCAT_UTIL_SET_REPLAGENT
1 数据库名
2 密码
3 创建归档
4 转储日志
5 页大小
11 内部错误(%d): 通讯错误
12 内部错误(%d): 无法申请内存
13 内部错误(%d): IO 错误
14 内部错误(%d): 通用
15 内部错误(%d): 无法 unzip 日志数据
16 内部错误(%d): 查询错误:%s
21 错误(%d): 无法连接到repl_server. 请检测端口号和repl_server进程的状态.
22 错误(%d): 无法打开拷贝日志文件 - %s.
23 错误(%d): 无法创建拷贝日志的归档文件 - %s
24 错误(%d): 无法打开目录文件.
25 错误(%d): 访问跟踪日志文件时出错. - %s
26 错误(%d): 无法连接到master. 请检查repl_agent是否已经在运行或master是否运行.
27 错误(%d): 无法登陆到slave. - %s
28 错误(%d): 无法连接到slave. - %s
29 错误(%d): Slave 数据库停止 - %s
30 错误(%d): Replication 被损坏 - %s
31 错误(%d): 无法从repl_server得到代理标识符. 请检测repl_server的状态.
32 错误(%d): 无法从repl_server得到日志头. 请检测repl_server的状态.
33 错误(%d): 无法从repl_server得到日志页. 请检测repl_server的状态.
34 错误(%d): 无法从distributor DB 得到参数.
35 错误(%d): 无法登陆分配者数据库. - %s
36 错误(%d): 无法连接到分配者数据库. - %s
37 错误(%d): 太多的slave注册到了分配者数据库的slave_info类中
38 错误(%d): 对于目标slave没有master信息. 请检测分配者数据库的trail_info类.
39 错误(%d): 太多的master注册到了分配者数据库的master_info类
40 错误(%d): 无效的跟踪信息. 请检查分配者数据库的trail_info类
41 错误(%d): 系统单方面的终止. 尝试增加max_block_size和max_block_count(cubrid.conf).
51 日志(%d): repl_agent进程正在重新启动.
52 日志(%d): 一项replication更新日志记录被转换到插入日志.
53 日志(%d): %s

$set 37 MSGCAT_UTIL_SET_PLANDUMP
15 无法打开输出文件 `%s'\n
59 plandump 无法运行在单机模式.\n
60 \
plandump: 显示或控制缓存的查询计划.\n\
usage: %s plandump [选项] 数据库名\n\
\n\
有效选项:\n\
  -d, --drop                   丢弃服务端缓存中的所有计划\n\
  -o, --output-file=FILE       重定向输出信息到FILE; 默认: 空\n

$set 38 MSGCAT_UTIL_SET_DUMPPARAM
11 无法打开输出文件 `%s'\n
21 系统参数将被客户端程序使用.\n
22 系统参数被服务 '%s' 读取\n
23 系统参数将被用于单机模式.\n
60 \
paramdump: 转储系统参数设置.\n\
usage: %s paramdump [选项] 数据库名\n\
\n\
有效选项:\n\
  -o, --output-file=FILE       重定向输出信息到FILE; 默认: 无\n\
  -b, --both                   转储客户端和服务端的参数\n\
  -S, --SA-mode                单机模式执行\n\
  -C, --CS-mode                客户端-服务端模式执行\n

$set 39 MSGCAT_UTIL_SET_CHANGEMODE
11 错误的值 \"%s\" 对于服务端的HA运行模式.\n
12 无法改变服务端的HA运行模式.\n
21 输入DBA密码:
22 服务 `%s'的当前HA运行模式是 %s.\n
23 服务 `%s'的 HA运行模式被改变为\"%s\".\n
24 服务没有被设置为HA模式.\n
58 HA 特性在这个平台上不被支持.\n
59 changemode 无法以单机模式运行.\n
60 \
changemode: 显示或改变服务的HA运行模式.\n\
usage: %s changemode [选项] 数据库名\n\
\n\
有效选项:\n\
  -m, --mode=MODE             将服务模式改变为MODE; 默认: 空\n\
  -w, --wait                  等到模式被改变\n\
  -f, --force                 强制改变\n

$set 40 MSGCAT_UTIL_SET_COPYLOGDB
11 错误的值 \"%s\" 对于copylogdb的运行模式.\n
21 输入DBA的密码:
22 服务被设置为HA模式.\n
58 HA 特性在这个平台上不被支持.\n
59 copylogdb 无法以单机模式运行.\n
60 \
copylogdb: 从服务端拷贝日志页.\n\
usage: %s copylogdb [选项] 数据库名\n\
\n\
有效选项:\n\
  -L, --log-path=PATH         指定目录PATH用于拷贝日志卷\n\
  -m, --mode=MODE             操作模式; 默认: 同步\n\
                              模式: 同步 或 半同步 或 异步\n

$set 41 MSGCAT_UTIL_SET_APPLYLOGDB
21 输入DBA密码:
22 服务没有被设置为HA模式.\n
58 HA 特性在这个平台上不被支持.\n
59 applylogdb 无法以单机模式运行.\n
60 \
applylogdb: 将已拷贝的日志应用到服务端.\n\
usage: %s applylogdb [选项] 数据库名\n\
\n\
有效选项:\n\
  -L, --log-path=PATH         将被读取的事务日志的目录\n\
      --max-mem-size=SIZE     进程可用的最大内存数, 以MB为单位; 最大为 1000\n

$set 43 MSGCAT_UTIL_SET_STATDUMP
11 无法打开输出文件 `%s'\n
59 statdump 无法以单机模式运行.\n
60 \
statdump: 转储全局统计信息.\n\
usage: %s statdump [选项] 数据库名\n\
\n\
有效选项:\n\
  -o, --output-file=FILE       重定向输出信息到文件FILE; 默认: 无\n\
  -c, --cumulative             不清理状态\n\
  -i, --interval=S             每 S 秒打印一次状态\n
