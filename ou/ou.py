#! /home/xiewj/.pyenv/shims python2.7
# -*- coding: utf-8 -*-

import os
import sys
import re
import csv
import time
from optparse import OptionParser

import pandas as pd
import numpy as np
import cx_Oracle as co
from jinja2 import Template

from base_.log_ import mylog
from base_.config_ import ORA_CONNECTION
sys.path.append('/home/xiewj/libraries/')
from base_.code_ import judge_code

logger = mylog()

def get_tb_name(infile):
    ''' get table name.
    Parameter
    -------------------------
    infile : str ,filename
    '''
    if isinstance(infile, str):
        # 文件名支持绝对路径和相对路径
        if os.path.isfile(infile):
            filename, extention = os.path.splitext(infile)
            table_name = os.path.basename(filename)
            if extention != '.csv':
                logger.debug('not a csv file!')
                sys.exit()
        else:
            logger.debug('file %s not exits!' % infile)
            sys.exit()
    else:
        logger.debug('the filename is not a string!')
        sys.exit()
    return table_name

def get_options():
    ''' get options '''
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', action='store', dest='filename', help='csv file to upload')
    parser.add_option('-d','--dlm', action='store', dest='delimiter', help='delimiter of csv file')
    parser.add_option('-e', '--encoding', action='store', dest='encoding', help='encoding of csv file')
    parser.add_option('-a', '--append', action='store', dest='appending', help='the way of upload, default create new table else insert')
    parser.add_option('-l','--col', action='store', dest='col_exclude_lst', help='the columns not to upload')
    parser.add_option('-c','--connection', action='store', dest='ora_connection', help='connect oracle with formatter "username/password[@domain]/database"')
    options, args = parser.parse_args()

    return options

def ou(infile, conn, encoding=None, delimiter='\t', appending=False, col_exclude_lst=None):
    ''' upload csv file into oracle database with creating or insert table.
    Parameter
    -----------------------------------
    infile : str ,csv file
    conn : str ,formatter "username/password[@domain]/database"
    encoding : str default None ,Encoding to use for UTF when reading/writing (ex. ‘utf-8’)
    delimiter : str default ','
    appending : boolean default False, if table exit,drop it and create new table else just create new. if True, insert
    col_exclude_lst : list default None, the column not to upload
    '''

    table_name = get_tb_name(infile)

    try:
        conn = co.connect(conn)
    except co.DatabaseError,e:
        logger.error(e)
        sys.exit()

    cs = conn.cursor()

    if encoding:
        code = encoding
    else:
        code = judge_code(infile)

    df = pd.read_csv(infile, delimiter=delimiter, encoding='gbk', dtype=np.object)
    df.fillna('', inplace=True)

    if col_exclude_lst:
        cols = list(set(df.columns) - set(col_exclude_lst))
    else:
        cols = list(df.columns)

    if col_exclude_lst:
        df = df[cols]

    # 支持sql*plus下载的csv文件读入,生成指定类型的数据
    sql_type = {}
    new_cols = []
    if '|' in cols[0]:
        for col in cols:
            col, type = col.split('|')
            sql_type[col] = type
            new_cols.append(col)

    if new_cols:
        cols = new_cols
        df.columns = new_cols

    # 如果不为appending模式，则创建新表
    if not appending:
        try:
            cs.execute('drop table %s' % table_name)
        except co.DatabaseError,e:
            logger.error(e)

        table_template = """ create table {{table_name}} (
        {%- for col in cols: -%}
            {% if loop.index0 == 0 %}
                {{col}} varchar2(100)
            {% else %}
                ,{{col}} varchar2(100)
            {% endif %}
        {%- endfor -%}
        )
        """
        create_tb_sql = Template(table_template)
        create_tb_sql = create_tb_sql.render(table_name=table_name, cols=cols)
        try:
            cs.execute(create_tb_sql)
        except co.DatabaseError,e:
            logger.error(e)
            conn.close()
            sys.exit()
    else:
        try:
            cs.execute('select * from %s' % table_name)
        except co.DatabaseError,e:
            logger.error(e)
            logger.info(u'表或者视图不存在,无法插入')
            conn.close()
            sys.exit()

    for ii, ix in enumerate(df.index):
        insert_value = """
            insert into {{table_name}} values 
            (
            {% for val in values %}
                {% if loop.index0 == 0 %}
                    '{{val}}'
                {% else %}
                    ,'{{val}}'
                {% endif %}
            {% endfor %}
            )
            """
        insert_value = Template(insert_value)
        insert_value = insert_value.render(table_name=table_name, values=df.iloc[ii])
        try:
            cs.execute(insert_value)
        except co.DatabaseError,e:
            logger.error(e)
            conn.close()
            sys.exit()
        cs.execute('commit')

    if sql_type:
        alter_template = """ alter table {{table_name}} modify 
            (
            {% for col, type in sql_type.iteritems(): %}
                {% if loop.index0 == 0 %}
                    {{col}} {{type}}
                {% else %}
                    ,{{col}} {{type}}
                {% endif %}
            {% endfor %}
            )
            """
        cg_dtype_sql = Template(alter_template)
        cg_dtype_sql = cg_dtype_sql.render(table_name=table_name, sql_type=sql_type)

        try:
            cs.execute(cg_dtype_sql)
        except co.DatabaseError,e:
            logger.error(e)
            conn.close()
            sys.exit()
        cs.execute('commit')


if __name__ == '__main__':

    options = get_options()
    infile = options.filename
    connection = options.ora_connection

    if ORA_CONNECTION.get(connection):
        connection = ORA_CONNECTION.get(connection)

    if not infile:
        logger.info('please input a file name')
        sys.exit()

    if not connection:
        logger.info('please input a database connection')
        sys.exit()

    encoding = options.encoding
    delimiter = options.delimiter if options.delimiter else '\t'
    appending = options.appending
    col_exclude_lst = options.col_exclude_lst

    ou(infile=infile, conn=connection, encoding=encoding, delimiter=delimiter
        , appending=appending, col_exclude_lst=col_exclude_lst)























