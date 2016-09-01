# -*- coding: utf-8 -*-

import os
import sys
import re
import csv
from jinja2 import Template

import pandas as pd
import numpy as np
import cx_Oracle as co
import chardet

from log import logger


def ou(infile):

    try:
        conn = co.connect('qf_scratch_ds/dsscqf123@172.16.1.155/dwprd')
    except co.DatabaseError,e:
        logger.error(e)
        sys.exit()

    cs = conn.cursor()

    try:
        if isinstance(infile, str):
            table_name = re.search(r'(.*?).csv$', infile).groups(0)[0]
        else:
            logger.debug('the filename is not a string!')
            sys.exit()
    except AttributeError,e:
        logger.error(e)
        sys.exit()

    def judge_code(infile, delimiter=','):
        df = pd.read_csv(infile, delimiter=delimiter, nrows=90)
        for col in df.columns:
            if isinstance(df.iloc[0][col], (np.string_,np.object)):
                code = chardet.detect(''.join(df[col]))['encoding']
                if code != 'ascii':
                    return code
        return 'ascii'

    code = judge_code(infile)

    with open(infile, 'r') as f:
        try:
            cs.execute('drop table %s' % table_name)
        except co.DatabaseError,e:
            logger.error(e)
            sys.exit()
        
        for ii, line in enumerate(f):
            line = unicode(line, code)
            if ii == 0:
                table_column_str = line
                table_column_lst = line.split(',')
                table_template = """ create table {{table_name}} (
                    {%- for col in table_column_lst: -%}
                        {% if loop.index0 == 0 %}
                            {{col}} varchar2(100)
                        {% else %}
                            ,{{col}} varchar2(100)
                        {% endif %}
                    {%- endfor -%}
                    );
                    """
                table_template = Template(table_template)
                table_crt_sql = table_template.render(table_column_lst=table_column_lst, table_name=table_name)
                try:
                    cs.execute(table_crt_sql)
                except co.DatabaseError,e:
                    logger.error(e)
                    sys.exit()
            else:
                if line:
                    insert_value = """
                        insert into {{table_name}} columns (
                        {% for col in table_column_lst %}
                            {% if loop.index0 == 0 %}
                                {{col}}
                            {% else %}
                                ,{{col}}
                            {% endif %}
                        {% endfor %}
                        ) 
                        values (
                        {% for val in line %}
                            {% if loop.index0 == 0 %}
                                '{{val}}'
                            {% else %}
                                ,'{{val}}'
                            {% endif %}
                        {% endfor %}
                        )
                        """
                    
                    insert_value = Template(insert_value)
                    insert_value = insert_value.render(table_name=table_name, table_column_lst=table_column_lst, line=line.split(','))
                    try:
                        cs.execute(insert_value)
                    except co.DatabaseError,e:
                        logger.error(e)
                        sys.exit()
        cs.execute('commit')
        
        



if __name__ == '__main__':
    ou('b.csv')
























