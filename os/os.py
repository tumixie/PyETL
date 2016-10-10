#! /usr/bin/env/python2.7
# -*- coding:utf-8 -*-

from optparse import OptionParser
import os
import sys
import re

from jinja2 import Template
from chardet.universaldetector import UniversalDetector
import cx_Oracle as co
import sqlparse
import chardet

from config import ORA_CONNECTION
from log import mylog

def judge_code(filename):
    with open(filename, 'r') as f:
        file_lst = f.readlines()
        detector = UniversalDetector()
        for line in file_lst:
            detector.feed(line)
            if detector.done:
                break
        detector.close()
    code = detector.result['encoding']
    return code

def check_file(infile, logger):
    ''' check the input file.
    Parameter
    -------------------------
    infile : str ,filename
    '''
    # 文件名支持绝对路径和相对路径
    if os.path.isfile(infile):
        filename, extention = os.path.splitext(infile)
        filename = os.path.basename(filename)
        if extention != '.sql':
            logger.debug('not a csv file!')
            return False
        return True
    else:
        logger.debug('file %s not exits!' % infile)
        return False

def get_options():
    ''' get options '''
    usage = "usage: %prog [options] arg1 arg2"
    parser = OptionParser(usage=usage)
    parser.add_option('-f', '--file', dest='filename', help='sql file to execute')
    parser.add_option('-e', '--encoding', action='store', dest='encoding', help='encoding of sql file')
    parser.add_option('-a', '--args', action='append', dest='arguments'
                      , help='sql file arguments with formatter "argname=argvalue"')
    parser.add_option('-c','--connection', action='store', dest='ora_connection'
                      , help='connect oracle with formatter "username/password[@domain]/database"')
    options, args = parser.parse_args()

    return options

def variable_parse(args_lst):
    '''暂时只支持字符串类型 '''
    arg_dict = {}
    for arg in args_lst:
        arg_sp = arg.split('=')
        arg_dict[arg_sp[0]] = arg_sp[1]
    return arg_dict

def ora_execute(conn, cursor, statement, logger):

    if statement:
        try:
            cursor.execute(statement)
            cursor.execute('commit')
        except co.DatabaseError,e:
            logger.error(e)

def get_sql(statement, args=None):

    statement = statement.strip()
    if not args:
        return statement
    else:
        sql_template = Template(statement)
        sql_statement = sql_template.render(args)
        return sql_statement

def _os_(infile, connection, logger, encoding=None, args_lst=None):

    if not check_file(infile, logger):
        sys.exit()

    if args_lst:
        arg_dict = variable_parse(args_lst)
    else:
        arg_dict = None

    code = encoding if encoding else judge_code(infile)
    logger.info(code)
    with open(infile, 'r') as f:
        sql_lst = f.readlines()

        try:
            conn = co.connect(connection)
        except co.DatabaseError,e:
            logger.info(connection)
            logger.error(e)
            sys.exit()
        cursor = conn.cursor()

        sql_str = ' '.join(sql_lst)
        sql_str = unicode(sql_str, code)

        sql_str = get_sql(sql_str, args=arg_dict)
        sql_statement_lst = sqlparse.parse(sql_str)

        for sql in sql_statement_lst:
            sql = unicode(sql)
            sql = sql.replace(';',' ')
            logger.info(sql)
            ora_execute(conn=conn, cursor=cursor, statement=sql, logger=logger)


if __name__ == '__main__':

    options = get_options()

    filename = options.filename
    encoding = options.encoding
    args_lst = options.arguments

    connection = options.ora_connection
    if not filename:
        print 'please input a filename!'
        sys.exit()
    else:
        logger = mylog(logFileName=filename)

    if not connection:
        print 'please input a database connection!'
        sys.exit()
    if ORA_CONNECTION.get(connection):
        connection = ORA_CONNECTION.get(connection)

    _os_(infile=filename, connection=connection, encoding=encoding, args_lst=args_lst, logger=logger)



























