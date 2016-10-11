# PyETL
***
##ETL tools for pythoner
###oe.py
>oracle数据库中下载表数据,并保存为csv文件

>python 版本：python2.7

>依赖的第三方库jinja2,cx_Oracle,sqlparse,pandas,numpy

在shell 或者 cmd中直接运行：
`python oe.py -c <username>/<password>[@<host>]/<dbname> -t <table_name>`

* `-c <username>/<password>[@<host>]/<dbname>` 
>【必要参数】str，oracle服务器连接,可以通过配置base_/config_.py文件文件来简化命令

* `-t <table_name>`
>【必要参数】str，待下载的oracle表，只能下载当前oracle用户下的表

* `-d <destination>`
>【可选参数】str，csv文件的保存位置，默认为当前目录

* `-l <exclude_cols_lst>`
>【可选参数】list，待下载表中不要的字段，默认全部下载。

* `-v/-q`
>【可选参数】boolean，默认为-v，下载后改变原oracle表中‘*LOB’类型的数据为varchar2类型
-q 不改变oracle表中的数据类型

###ou.py
>支持csv文件上传至oracle数据库，生成对于的表，支持多种上传模式

`python ou.py -c <username>/<password>[@<host>]/<dbname> -f <filename>`

* `-c <username>/<password>[@<host>]/<dbname>`
>【必要参数】str，oracle服务器连接,可以通过配置base_/config_.py文件文件来简化命令

* `-f <filename>`
>【必要参数】str， 上传的csv文件

* `-l <exclude_cols_lst>`
>【可选参数】list，待上传csv文件中不上传的字段，默认全部上传。

* `-a/-q`
>【可选参数】boolean，默认为-q，若数据库中存在表则先删除再创建，-a则再原表中添加记录。

* `-d <delimiter>`
>【可选参数】str, 待上传csv文件的分隔符，默认为制表符'\t'。

* `-e <encoding>`
>【可选参数】str，待上传csv文件的编码方式，默认使用utf-8。

###os_.py
`python os_.py -c <username>/<password>[@<host>]/<dbname> -f <filename>`
>可直接执行sql脚本。使用jinja2模板渲染，支持sql脚本传入参数。<a href="http://docs.jinkan.org/docs/jinja2/" target="_blank">jinja2</a>

* `-c <username>/<password>[@<host>]/<dbname>`
>【必要参数】str，oracle服务器连接,可以通过配置base_/config_.py文件文件来简化命令

* `-f <filename>`
>【必要参数】str， 待执行的sql脚本

* `-e <encoding>`
>【可选参数】str，待执行的sql脚本的编码方式，默认使用utf-8。

* `-a <arguments>`
>【可选参数】list，传入sql脚本中的参数，格式为`argument=value`
