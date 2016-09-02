#-*- coding: utf-8 -*-
u'''
连接 oracle 数据库, 获取数据
'''
import os
import time
import chardet

import pandas as pd
import cx_Oracle as co
from jinja2 import Template

os.environ['NLS_LANG']='SIMPLIFIED CHINESE_CHINA.UTF8'


def get_scorecard_data(table_name, DATA_TARGET_PATH = 'DATA/', exclude_lst = None, CONNECTION = None):
    '''
    parameters:
    -----------
    table_name                                 str
        oracle 中的表名
    DATA_TARGET_PATH                        str
        文件输出 目录
    exclude_lst                                list
        list of str     需要排除的变量
    CONNECTION                                 str
        <username>/<password>@<host>/<dbname>

    results:
    --------
    输出 文件 {{ 表名 }} 作为数据(\t, 首行格式为 {{COLUMN_NAME}}|{{DATA_TYPE}})
            {{ 表名_label.csv }} 作为元数据, 首行格式为 COLUMN_NAME,TABLE_NAME,DATA_TYPE,DATA_LENGTH
        两个文件的变量 顺序是一致

    attention:
    ----------
    label 文件需要再手工修改
    '''
    os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
    table_name = str.upper(table_name)
    if exclude_lst is None:
        exclude_lst = []
    exclude_lst = [str.upper(x) for x in exclude_lst]
    
    conn = co.connect(CONNECTION)
    cursor = conn.cursor()
    
    # 获取 元数据
    # '%s' 而不是 "%s"
    sql_meta = '''select TABLE_NAME, COLUMN_NAME, DATA_TYPE, DATA_LENGTH from USER_TAB_COLS
                  where TABLE_NAME like '%s'
               ''' % table_name
    df_meta = pd.read_sql(sql_meta, conn)
    df_meta.set_index(['COLUMN_NAME'], inplace = True)

    variable_lst = list(df_meta.index)
    variable_lst = list(set(variable_lst) - set(exclude_lst))
    df_meta = df_meta.ix[variable_lst,]
    df_meta_print = df_meta.copy()
    df_meta_print.reset_index(inplace = True)

    #df_meta_print.to_csv(DATA_TARGET_PATH + table_name + '_label.csv', index = False, sep =',', encoding='utf-8')

    # 数据
    sql_data = Template("""select 
        {% for column in variable_lst %}
            {% if loop.index0 == 0 %}
                {{ column }}
            {% else %}
                ,{{ column }}
            {% endif %}
        {% endfor %}
        from {{ table }}
        """)
    #print sql_data
    sql_data = sql_data.render(variable_lst = variable_lst, table=table_name)
    df_data = pd.read_sql(sql_data, conn)

    #df_data.rename(columns = {x: x + '|' + df_meta.ix[x, 'DATA_TYPE'] for x in variable_lst}, inplace = True)


    df_data.to_csv(DATA_TARGET_PATH + table_name + '.csv', index = False, sep ='\t', encoding='utf-8')

if __name__ == '__main__':
    get_scorecard_data('qf_risk_loan_issue_fact',DATA_TARGET_PATH=r'C:/Users/weijiexie/Desktop/')

