# -*- coding: utf-8 -*-

from base_.log_ import mylog
from base_.config_ import ORA_CONNECTION

logger = mylog('a.txt')


def main():
    logger.info('ok')

main()