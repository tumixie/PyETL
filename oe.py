#! python2.7
# -*- coding: utf-8 -*-

"""
连接 oracle 数据库, 获取表数据
"""

import os
import time
import sys
import csv
from optparse import OptionParser

import pandas as pd
import cx_Oracle as co
from jinja2 import Template

from base_.config_ import ORA_CONNECTION
from base_.code_ import judge_code
from base_.log_ import mylog


logger = mylog('oe.log')

def get_options():
    """ get options """
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage=usage)
    parser.add_option('-t', '--table', action='append', dest='table_name_lst', help='table to extract')
    parser.add_option('-d', '--dest', action='store', dest='destination', help='path to store, default current path')
    parser.add_option('-l','--col', action='append', dest='col_exclude_lst', help='the columns not to upload')
    parser.add_option('-v', action='store_true', dest="verbose", default=False, help='transfer CLOB/BLOB data to varchar2')
    parser.add_option('-q', action='store_false', dest="verbose", default=True, help='boolean')
    parser.add_option('-c','--connection', action='store', dest='ora_connection'
                     , help='str, connect oracle with formatter "username/password[@domain]/database"')
    options, args = parser.parse_args()

    return options


def oe(table_name, connection, data_target_path=None, exclude_lst = None, dtype_transfer=None):
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

    usage:
    shell 或者 cmd下直接python调用
    `python oe.py -c <username>/<password>@<host>/<dbname> -t <table_name> -v`
    支持在base_.config_中添加配置oracle服务器连接，简化命令行操作。
    
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
        print u'未提供表名'
        sys.exit()
        conn.close()
    elif len(df_meta) == 0:
        print u'表或试图不存在'
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
    print code
    column_lst = [unicode(x, code) for x in column_lst]

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
    out_file_name = data_target_path + table_name + '.csv'
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
    print end_time - start_time
    logger.info('run time: %s' % str(end_time - start_time))

if __name__ == '__main__':
    options = get_options()
    table_name_lst = options.table_name_lst
    destination = options.destination
    connection = options.ora_connection
    col_exclude_lst = options.col_exclude_lst
    dtype_transfer = options.verbose

    if len(table_name_lst) == 0:
        print 'please input table which you want to extract from database'
        sys.exit()

    if not connection:
        print 'please input oracle connection'
        sys.exit()

    if ORA_CONNECTION.get(connection):
        connection = ORA_CONNECTION.get(connection)

    if not destination:
        destination = ''

    for t_name in table_name_lst:
        logger.info(t_name)
        logger.info('--------------------')
        oe(t_name, connection=connection, data_target_path=destination, exclude_lst=col_exclude_lst
          , dtype_transfer=dtype_transfer)
        logger.info('--------------------')

