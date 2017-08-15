from scrapy import signals


class SqlScripts(object):

    def __init__(self, settings):
        pass

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.settings)
        # crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    @staticmethod
    def _get_cnxn(spider):
        # Get connection so we can update the retail_runlog status if problem
        cnxn = None
        spider.logger.debug('DB_TYPE is %s' % spider.settings.get('DB_TYPE'))
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

    def spider_closed(self, spider):
        '''
        Method called when the spider is closed responsible for executing sql scripts
        defined for this target
        Args:
            self --- Instance of the extension
            spider --- A Scrapy spider instance
        '''

        # Get the SQL scripts user has defined for this target site if any
        try:
            sqlscripts = spider.target_site.sqlscripts.all()
            spider.logger.debug('Got %i sql scripts to run' % len(sqlscripts))
        except AttributeError:
            spider.logger.error('The spider has no target_site attrib or target_site has no sqlscripts attrib...')
            return None

        ########################################################################
        # Get connection
        ########################################################################
        cnxn = self._get_cnxn(spider)
        if cnxn is not None:
            curs = cnxn.cursor()

            for sqlscript in sqlscripts:
                try:
                    spider.logger.debug('Exec sql script %s' % sqlscript.cmd.replace('<RUN_ID>', str(spider.RUN_ID)))
                    curs.execute(sqlscript.cmd.replace('<RUN_ID>', str(spider.RUN_ID)))
                except Exception as exc:
                    spider.logger.error('Exception when running custom SQL check: %s'
                                        ' The exception was:\n %s' % (sqlscript.cmd.replace('<RUN_ID>', str(spider.RUN_ID)),
                                                                      exc))
            cnxn.commit()
