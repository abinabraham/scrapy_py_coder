from __future__ import division
from scrapy import signals


class RetailSpiderStatus(object):
    '''
    Very simple extension that does nothing other than mark the retail_run_log
    row 'spider_status' as 'finished'.

    Need to add this in EXTENSIONS under settings.py
    loading order is not so important for extensions
    EXTENSIONS = {
        'scrapy_evo_common.extensions.retail_jobstatus.RetailJobStatus' : 500,
    }
    '''

    @classmethod
    def from_crawler(cls, crawler):
        o = cls()
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        return o

    @staticmethod
    def _get_cnxn(spider):
        # Get connection so we can update the retail_runlog status if problem
        cnxn = None
        if spider.settings.get('DB_TYPE') == 'MYSQL':
            import MySQLdb
            dbargs = dict(
                host=spider.settings['DB_HOST'],
                db=spider.settings['DB_NAME'],
                user=spider.settings['DB_USER'],
                passwd=spider.settings['DB_PWD'],
                charset='utf8',
                use_unicode=True,
            )
            try:
                cnxn = MySQLdb.connect(**dbargs)
            except Exception as cn_exc:
                # We could raise one of these
                # http://docs.celeryproject.org/en/latest/userguide/tasks.html#semipredicates
                spider.logger.error(cn_exc)
        elif spider.settings.get('DB_TYPE') == 'MSSQL':
            import pyodbc
            dbargs = dict(
                TDS_Version=7.2,
                DRIVER='{FreeTDS}',
                SERVER=spider.settings['DB_HOST'],
                PORT=spider.settings['DB_PORT'],
                DATABASE=spider.settings['DB_NAME'],
                UID=spider.settings['DB_USER'],
                PWD=spider.settings['DB_PWD'],
            )
            try:
                cnxn = pyodbc.connect(**dbargs)
            except Exception as cn_exc:
                spider.logger.error(cn_exc)
        return cnxn

    def spider_opened(self, spider):
        '''
        Method is called upon spider opened.

        Args:
            self --- Extension instance
            spider --- Scrapy spider instance
        '''
        spider.logger.debug('Starting retail_jobstatus spider opened RetailJobStatus....')

        # Get connection so we can update the retail_runlog status if problem
        cnxn = self._get_cnxn(spider)

        # Set the retail_run_log job status
        if cnxn is not None:
            spider.logger.debug('Updating retail run log status to scrape started')
            curs = cnxn.cursor()
            curs.execute("UPDATE retail_run_log SET spider_status='%s' WHERE run_id=%s;"
                         % ('scrape started', spider.RUN_ID))
            cnxn.commit()
            cnxn.close()

    def spider_closed(self, spider):
        '''
        Method is called upon spider close.

        Args:
            self --- Extension instance
            spider --- Scrapy spider instance
        '''
        spider.logger.debug('Starting retail_jobstatus spider closed RetailJobStatus....')

        # Get connection so we can update the retail_runlog status if problem
        cnxn = self._get_cnxn(spider)

        # Set the retail_run_log job status
        if cnxn is not None:
            spider.logger.debug('Updating retail run log status to scrape finished')
            curs = cnxn.cursor()
            curs.execute("UPDATE retail_run_log SET spider_status='%s' WHERE run_id=%s;"
                         % ('scrape finished', spider.RUN_ID))
            cnxn.commit()
            cnxn.close()
