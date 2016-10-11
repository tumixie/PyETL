# PyETL
***
##ETL tools for pythoner
###oe
>oracle数据库中下载表数据,并保存为csv文件

在shell 或者 cmd中直接运行：
`python oe.py -c <username>/<password>[@<host>]/<dbname> -t <table_name>`

* `-c <username>/<password>[@<host>]/<dbname>` 
>oracle服务器连接,可以通过配置base_/config_.py文件文件来简化命令

* `-t <table_name>`
>待下载的oracle表，只能下载当前oracle用户下的表

* `-d <destination>`
>【可选参数】str，csv文件的保存位置，默认为当前目录

* `-l <exclude_cols_lst>`
>【可选参数】list，待下载表中不要的字段，默认全部下载。

* `-v/-q`
>【可选参数】boolean，默认为-v，下载后改变原oracle表中‘*LOB’类型的数据为varchar2类型
-q 不改变oracle表中的数据类型


