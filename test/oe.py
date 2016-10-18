#! python2.7
# -*- coding: utf-8 -*-

"""
连接 oracle 数据库, 获取表数据
"""

import os
import time
import sys
import csv
import threading

import pandas as pd
import cx_Oracle as co
from jinja2 import Template
from docopt import docopt

from base_.config_ import ORA_CONNECTION
from base_.code_ import judge_code
from base_.log_ import mylog


logger = mylog('oe.log')

options = \
"""
    Usage:
      script.py -c <conn> [--d=<path>] [--l=<ex_col>] [-v] [--s=<sep>] [--f=<ftype>] -t <table>... 
      script.py (-h | --help)
      script.py --version

    Examples:
      myscript.py -c zhangsan/zs123@quark.com/qf_risk -t test_table

    Options:
      -h --help      show help message
      --verison      show version
      -c             oracle connection with formatter `<username>/<password>[@<host>]/<dbname>`
      -t             download table
      --d=<path>     path to save file
      --l=<ex_col>   table columns not to download
      --s=<sep>      separation of download file, default `\t`
      --f=<ftype>    type of download file,only could be one of [`csv`],default `csv`
      -v             the download method. if true,transfer download datatype like `*LOB` to `VARCHAR2`
      
"""


def multi_process(func_lst):
    """ func_lst with formater `[(func, args)...]`"""
    thread_lst = [threading.Thread(target=func, args=args) for func, args in func_lst]
    for thread in thread_lst:
        thread.start()
    for thread in thread_lst:
        thread.join()


def oe(table_name, connection, data_target_path=None, exclude_lst = None, dtype_transfer=None, delimiter='\t', ftype=None):
    """
    parameters:
    -----------
    table_name                                 str
        oracle 中的表名
    data_target_path                           str
        文件输出 目录
    exclude_lst                                list
        list of str     需要排除的变量
    connection                                 str
        <username>/<password>@<host>/<dbname>
    dtype_transfer                             boolean
        if True

    results:
    --------
    输出 csv文件
    """

    start_time = time.time()
    conn = co.connect(connection)
    cursor = conn.cursor()

    # 获取 元数据
    # '%s' 而不是 "%s"
    sql_meta = '''select TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH from USER_TAB_COLS
                  where TABLE_NAME like '%s' order by column_id
               ''' % str.upper(table_name)
    df_meta = pd.read_sql(sql_meta, conn)
    
    if df_meta is None:
        print('未提供表名')
        sys.exit()
        conn.close()
    elif len(df_meta) == 0:
        print('表或试图不存在')
        sys.exit()
        conn.close()

    column_order_dict = {col: ix for ix, col in enumerate(df_meta['COLUMN_NAME'])}
    def get_order(value):
        return column_order_dict.get(value)

    if exclude_lst is not None:
        column_lst = list(set(df_meta['COLUMN_NAME']) - set(exclude_lst))
        column_lst = sorted(column_lst, key=get_order)
    else:
        column_lst = list(df_meta['COLUMN_NAME'])

    if dtype_transfer:
        column_lst = [('to_char(%s) as %s' %(col,col)) if 'LOB' in dtype else col
                     for col, dtype in zip(df_meta['COLUMN_NAME'], df_meta['DATA_TYPE'])]

    code = judge_code(column_lst)
    column_lst = [str(x, code) for x in column_lst]

    # 数据
    sql_data = Template("""
               select {% for col in column_lst -%}
                         {%- if loop.index0 == 0 -%}
                            {{col}}
                         {%- else -%}
                            ,{{col}}
                         {%- endif %}
                      {%- endfor %}
               from {{table}}""")
    sql_data = sql_data.render(table=table_name, column_lst=column_lst)

    data_target_path = '' if data_target_path is None else data_target_path
    ftype = '' if ftype == '' else ('.' + ftype)
    out_file_name = data_target_path + table_name + ftype
    if os.path.isfile(out_file_name):
        os.remove(out_file_name)
    if dtype_transfer:
        tb_data_df = pd.read_sql(sql=sql_data, con=conn, chunksize=1000)
        for chunk in tb_data_df:
            chunk.to_csv(out_file_name, sep='\t', encoding='utf-8', index=False, mode='a')
        #tb_data_df = pd.read_sql(sql=sql_data, con=conn)
        #tb_data_df.to_csv(out_file_name, sep='\t', encoding='utf-8', index=False, mode='a')
    else:
        tb_data_iter = cursor.execute(sql_data)

        with open(out_file_name, 'a') as f:
            csv_file = csv.writer(f, delimiter='\t')
            csv_file.writerow([x.encode('utf-8') for x in column_lst])

        while True:
            records = tb_data_iter.fetchmany(numRows=50)
            if records:
                records_df = pd.DataFrame(records)
                records_df.to_csv(out_file_name, encoding='utf-8', mode='a'
                                 , header=False, index=False, sep='\t')
            else:
                break
    end_time = time.time()
    print(end_time - start_time)
    logger.info('code: %s' % code)
    logger.info('[%s][run time]: %s' % (table_name, str(end_time - start_time)))


if __name__ == '__main__':
    arguments = docopt(options)
    conn = ORA_CONNECTION.get(arguments['<conn>']) if ORA_CONNECTION.get(arguments['<conn>']) else arguments['<conn>']
    table_lst = arguments['<table>']
    data_target_path = arguments['--d']
    exclude_cols_lst = arguments['--l']
    dtype_transfer = arguments['-v']
    delimiter = '\t' if arguments['--s'] is None else arguments['--s']
    ftype = '' if arguments['--f'] is None else arguments['--f']
    
    multi_oe_lst = [(oe, (tb, conn, data_target_path, exclude_cols_lst, dtype_transfer, delimiter, ftype)) for tb in table_lst]

    multi_process(multi_oe_lst)

