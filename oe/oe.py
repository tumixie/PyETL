#! python2.7
# -*- coding: utf-8 -*-

"""
连接 oracle 数据库, 获取数据
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


os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'


def get_options():
    """ get options """
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage=usage)
    parser.add_option('-t', '--table', action='append', dest='table_name_lst', help='str, table to extract')
    parser.add_option('-d', '--dest', action='store', dest='destination', help='str, csv file to store')
    parser.add_option('-l','--col', action='append', dest='col_exclude_lst', help='str, the columns not to upload')
    parser.add_option('-v', action='store_true', dest="verbose", default=True
                     , help='boolean, transfer CLOB/BLOB data to varchar at most 4000 bytes')
    parser.add_option('-q', action='store_false', dest="verbose", default=False
                     , help='boolean, not transfer CLOB/BLOB data to varchar at most 4000 bytes')
    parser.add_option('-c','--connection', action='store', dest='ora_connection'
                     , help='str, connect oracle with formatter "username/password[@domain]/database"')
    options, args = parser.parse_args()

    return options


def oe(table_name, connection, data_target_path='', exclude_lst = None, dtype_transfer=None):
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
    输出 文件 {{ 表名 }} 作为数据(\t, 首行格式为 {{COLUMN_NAME}}|{{DATA_TYPE}})
            {{ 表名_label.csv }} 作为元数据, 首行格式为 COLUMN_NAME,TABLE_NAME,DATA_TYPE,DATA_LENGTH
        两个文件的变量 顺序是一致

    attention:
    ----------
    label 文件需要再手工修改
    """
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

    start_time = time.time()
    conn = co.connect(connection)
    cursor = conn.cursor()

    # 获取 元数据
    # '%s' 而不是 "%s"
    sql_meta = '''select TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH from USER_TAB_COLS
                  where TABLE_NAME like '%s' order by column_id
               ''' % str.upper(table_name)
    df_meta = pd.read_sql(sql_meta, conn)
    if len(df_meta) == 0:
        print u'表或试图不存在'

    column_order_dict = {col: ix for ix, col in enumerate(df_meta['COLUMN_NAME'])}
    def get_order(value):
        return column_order_dict.get(value)

    if exclude_lst:
        column_lst = list(set(df_meta['COLUMN_NAME']) - set(exclude_lst))
        column_lst = sorted(column_lst, key=get_order)
    else:
        column_lst = list(df_meta['COLUMN_NAME'])

    transfer_col_lst = [row['COLUMN_NAME'] for ix, row in df_meta.iterrows() if 'LOB' in row['DATA_TYPE']]
    if dtype_transfer:
        column_lst = ['to_char(%s) as %s'% (col,col) if col in transfer_col_lst else col for col in column_lst]

    code = judge_code(''.join(column_lst))
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

    out_file_name = data_target_path + table_name + '.csv'
    if os.path.isfile(out_file_name):
        os.remove(out_file_name)
    if dtype_transfer or not transfer_col_lst:
        tb_data_df = pd.read_sql(sql_data, conn)
        tb_data_df.to_csv(out_file_name, sep='\t', encoding='utf-8', index=False)
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
        oe(t_name, connection=connection, data_target_path=destination, exclude_lst=col_exclude_lst
          , dtype_transfer=dtype_transfer)

