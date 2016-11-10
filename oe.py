#! python2.7
# -*- coding: utf-8 -*-

"""
    Usage:
      script.py -c <conn> [--p=<path>] [--l=<ex_col>] [-o] [--d=<sep>] [--f=<ftype>] -t <table>...
      script.py (-h | --help)
      script.py --version

    Examples:
      python myscript.py -c zhangsan/zs123@quark.com/qf_risk -t test_table

    Options:
      -h --help      show help message
      --verison      show version
      -c             oracle connection with formatter `oracle://<username>:<password>[@<host>]/<dbname>`
      -t             download table list
      -o             got csv-like file with columns like `<column>|<datatype>(<datalength>)`
      --p=<path>     path to save file
      --l=<ex_col>   table columns not to download, with formatter `<col1>,<col2>,...`
      --d=<sep>      separation of download file, default `\t`
      --f=<ftype>    type of download file,only could be one of [`csv`],default `csv`
      
"""

from __future__ import division
import os
import time
import sys
import threading

import pandas as pd
import sqlalchemy
from jinja2 import Template
from docopt import docopt
from collections import namedtuple

from base_.config_ import ORA_CONNECTION
from base_.code_ import judge_code
from base_.log_ import log_


logger = log_('oe.log', True)


def get_file(fdir, file, ftype):
    fdir = '' if fdir is None else (fdir + ('' if fdir.endswith('/') else '/'))
    ftype = '' if ftype is None else ('.' + ftype)
    return fdir + file + ftype
#

def get_cols(meta_df, ex_cols_str, sep=','):
    meta_df.index = meta_df['column_name']
    tb_field_lst = (list(meta_df['column_name']) if ex_cols_str is None else 
                   [x for x in meta_df['column_name'] if x not in map(str.upper, ex_cols_str.split(sep))])
    tb_dtype_lst = [(str(meta_df['data_type'][x]) + '(' + str(meta_df['data_length'][x]) + ')') for x in tb_field_lst]
    return namedtuple('fields', tb_field_lst)._make(tb_dtype_lst)
#

def get_query_sql(table, tb_fields):
    column_lst = [('to_char(%s) as %s' %(col,col)) if 'LOB' in col else col for col in tb_fields._fields ]
    code = judge_code(column_lst)
    logger.info('[code]: %s' % code)
    column_lst = [unicode(x, code) for x in column_lst]
    sql_data_tp = Template("""
               select {% for col in column_lst -%}
                        {%- if loop.index0 == 0 -%}
                          {{col}}
                        {%- else -%}
                          ,{{col}}
                        {%- endif %}
                      {%- endfor %}
               from {{table}} where rownum<=10""")
    sql_data = sql_data_tp.render(table=table, column_lst=column_lst)
    return sql_data
#

def to_file(conn, sql, filename, delimiter, columns=None):
    tb_df = pd.read_sql(sql=sql, con=conn, chunksize=2000)
    for ii, chunk in enumerate(tb_df):
        if ii == 0:
            columns = pd.DataFrame([chunk.columns if columns is None else columns])  
            columns.to_csv(filename, sep=delimiter, encoding='utf-8', index=False, header=None, mode='a')
        chunk.to_csv(filename, sep=delimiter, encoding='utf-8', index=False, header=None, mode='a')
#


#

def oe_(table_name, conn, exclude_cols = None, sep='\t', ftype=None, fpath=None, ora_col=False):
    """
    parameters:
    -----------
    table_name: `str`, table name for download
    conn: `str`, oracle connection with formatter `<username>/<password>@<host>/<dbname>`
    exclude_cols: `str`, undownload columns separated by `,`
    fpath: `str`, path to save download file, default current path
    ftype: `str`, download file type, default `csv`
    sep: `str`, csv-like file's separation

    results:
    --------
    csv-like file
    """

    start_time = time.time()
    conn = sqlalchemy.create_engine(conn)

    # 获取 元数据 '%s' 而不是 "%s"
    meta_sql = '''select TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH from USER_TAB_COLS
                  where TABLE_NAME like '%s' order by column_id
               ''' % str.upper(table_name)
    meta_df = pd.read_sql(meta_sql, conn)
    tb_fields = get_cols(meta_df, exclude_cols)
    data_sql = get_query_sql(table_name, tb_fields)
    out_file = get_file(fpath , table_name , ftype)
    if os.path.isfile(out_file):
        os.remove(out_file)

    # 数据
    out_cols = [(x + '|' + getattr(tb_fields, x)) if ora_col else x for x in tb_fields._fields]
    to_file(conn, data_sql, out_file, sep, out_cols)

    end_time = time.time()
    logger.info('[%s][run time]: %s' % (table_name, str(end_time - start_time)))


def multi_process(func_lst):
    """ func_lst with formater `[(func, args)...]`"""
    thread_lst = [threading.Thread(target=func, args=args) for func, args in func_lst]
    for thread in thread_lst:
        thread.start()
    for thread in thread_lst:
        thread.join()


if __name__ == '__main__':
    arguments = docopt(__doc__)
    conn = ORA_CONNECTION.get(arguments['<conn>']) if ORA_CONNECTION.get(arguments['<conn>']) else arguments['<conn>']
    table_lst = arguments['<table>']
    fpath = arguments['--p']
    exclude_cols = arguments['--l']
    delimiter = '\t' if arguments['--d'] is None else arguments['--d']
    ftype = arguments['--f']
    ora_col = arguments['-o']
    multi_oe_lst = [(oe_, (tb, conn, exclude_cols, delimiter, ftype, fpath, ora_col)) for tb in table_lst]

    multi_process(multi_oe_lst)

