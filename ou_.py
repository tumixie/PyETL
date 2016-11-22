#! /home/xiewj/.pyenv/shims python2.7
# -*- coding: utf-8 -*-

"""
    Usage:
        script.py -c <con> -f <infile> [--d=<sep>] [--e=<encoding>] [--l=<ex_cols>] [-a]
        script.py (-h|--help)
        script.py --version

    Examples:
        python myscript.py -f myinfile -c connection --d=, --e=utf-8

    Options:
        -f                  input csv-like file
        -c                  oracle connection with format like `username/password[@domain]/database`
        -a                  mode of upload, default appending
        --d=<sep>           delimiter of csv-like file
        --e=<encoding>      encoding of csv-like file
        --l=<ex_cols>       unupload columns
             
"""



import os
import sys
import re
import time

import pandas as pd
import numpy as np
import sqlalchemy
from jinja2 import Template
from docopt import docopt
from collections import OrderedDict

from base_.log_ import log_
from base_.config_ import ORA_CONNECTION, DTYPE_MAPPING
from base_.code_ import judge_code


logger = log_('ou.log', True)

def get_name(infile):
    if os.path.isfile(infile):
        filename, extention = os.path.splitext(infile)
        return os.path.basename(filename)
    else:
        logger.debug('file %s not exits!' % infile)
        sys.exit()
#

def get_cols(df, ex_cols):
    if ex_cols is None:
        return df
    df.columns = df.columns.map(lower)
    for col in ex_cols.split(','):
        if col.upper() in df.columns:
            df.drop(col.upper(), axis=1, inplace=True)
        elif col.lower() in df.columns:
            df.drop(col.lower(), axis=1, inplace=True)
    return df
#


def to_oracle(table, df, con, dtype, mode):
    df.to_sql(table, con, dtype=dtype, if_exists=mode, index=False, chunksize=2000)
#

def get_dtype(df, dtype=None):
    if dtype is None:
        if '|' not in list(df.columns)[0]:
            logger.warning(u'未指定列数据类型')
            dtype = {col: DTYPE_MAPPING['VARCHAR2'] for col in df.columns}
        else:
            dtype = {col.split('|')[0]: DTYPE_MAPPING[re.search('\|([a-zA-Z0-9]+)\(', col).groups()[0].upper()]
                     for col in df.columns}
    return dtype


def ou_(infile, conn, encoding=None, sep='\t', appending=False, ex_cols=None):
    ''' upload csv file into oracle database with creating or insert table.
    Parameter
    -----------------------------------
    infile : `str`, csv file
    conn : `str`, formatter "username/password[@domain]/database"
    encoding : `str`, default None ,Encoding to use for UTF when reading/writing (ex. `utf-8`)
    delimiter : `str`, default ','
    appending : `boolean`, default False, if table exit,drop it and create new table,if True, insert
    col_exclude_lst : 'str' default None, the column not to upload
    '''
    tb_name = get_name(infile)
    conn = sqlalchemy.create_engine(conn)
    code = encoding if encoding else judge_code(infile)
    it_df = pd.read_csv(infile, delimiter=sep, encoding=encoding, chunksize=200000, dtype=np.object)
    if_exists = 'append' if appending else 'replace'
    for ii, df in enumerate(it_df):
        df.fillna('', inplace=True)
        df = get_cols(df, ex_cols)
        dtype = get_dtype(df) if ii == 0 else dtype
        df.columns = df.columns.map(lambda x: x.split('|')[0])
        to_oracle(tb_name, df, conn, dtype, if_exists)



if __name__ == '__main__':
    arguments = docopt(__doc__)
    conn =  ORA_CONNECTION.get(arguments['<con>']) if ORA_CONNECTION.get(arguments['<con>']) else arguments['<con>']
    infile = arguments['<infile>']
    encoding = arguments['--e']
    sep = '\t' if arguments['--d'] is None else arguments['--d']
    appending = arguments['-a']
    ex_cols = arguments['--l']
    ou_(infile, conn, encoding, sep, appending, ex_cols)
    









