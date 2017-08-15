# The MYSQL Writer pipeline (not stored in pipelines.py because of the size)
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
from scrapy.exceptions import CloseSpider, NotConfigured
from scrapy.loader.processors import TakeFirst
from twisted.enterprise import adbapi
take_first = TakeFirst()


class BaseMySQLWriter(object):
    """
    A pipeline to store the item in a MYSQL database.

    This implementation uses Twisted's asynchronous database API.
    (non blocking). If you use a blocking db writer, the whole spider
    will pause until the block clears (try with time.sleep to simulate really slow
    db)

    Required settings:
        DB_TYPE  --- MYSQL or MSSQL
        DB_HOST  --- The db hostname
        DB_NAME  --- The db name
        DB_PORT  --- The db port
        DB_USER  --- The db user
        DB_PWD   --- The db password
    """

    def __init__(self, crawler_settings, crawler_stats):
        '''
        Init the MYSQLWriter instance. Make a connection to the database
        using the settings the user configured, and store some useful settings
        on the instance. If the user did not configre the required settings
        raise an excpetion

        Args:
            self --- The MYSQLWriter instance
            crawler_settings --- The crawler settings
        '''
        # Access to stats
        self.stats = crawler_stats
        try:
            # Image urls and paths fields
            self.images_urls_field = crawler_settings['IMAGES_URLS_FIELD']
            self.images_result_field = crawler_settings['IMAGES_RESULT_FIELD']

            # Dictionary specifying the connection arguments
            dbargs = dict(
                host=crawler_settings['DB_HOST'],
                db=crawler_settings['DB_NAME'],
                user=crawler_settings['DB_USER'],
                passwd=crawler_settings['DB_PWD'],
                charset='utf8',
                use_unicode=True,
            )
            # Store the dbargs on the instance so we can access it in process_item
            self.dargs = dbargs
            # Connect to the database
            self.dbpool = adbapi.ConnectionPool('MySQLdb', cp_reconnect=1, **dbargs)
            # Int the run_id
            self.run_id = None
        except KeyError:
            raise NotConfigured

    @classmethod
    def from_crawler(cls, crawler):
        '''
        See scrapy.middleware.py. If pipeline class has from_crawler
        or from settings construction with these is preferred.

        Args:
            cls --- The MYSQLWriter class
            crawler_settings --- The spider settings
        Returns:
            A configured instance of the MYSQLWriter class
        '''
        return cls(crawler.settings, crawler.stats)

    # Override to set table names
    def open_spider(self, spider):
        '''
        Just so we can read the table name on a per spider
        basis. If the spider instance doesn't have these names then we
        fall back to the defaults from settings.py

        Other attributes like merchant id and run id are also stored for
        future use. Note that these cannot be set during the init because they come
        from the spider, only available after spider opens.

        Args:
            self --- The MYSQLWriter instance
            spider --- The Scrapy spider instance
        '''
        try:
            # Set the marchant_id
            self.merchant_id = spider.merchant_id
            # Set run_id
            self.run_id = spider.run_id
        except AttributeError:
            raise CloseSpider('Spider did not have either merchant id or run id attrib')

        # Set table names

    def close_spider(self, spider):
        """
        Cleanup function, called after crawing has finished to close db
        connection pool.
        """
        self.dbpool.close()

    def process_item(self, item, spider):
        '''
        Process the item. Write it to db in the thread pool.
            > We call runInteraction to do the upsert (update-insert)
            > In the event of an exception the `_handle_error` errback is called
              And here is where any uncaught exceptions will be dealt with
            > In the event of error or success the lambda function is called
              which simply returns the item

        Args:
            self --- The MYSQLWriter instance
            item --- Item being processed that we wish to write to db
            spider --- Scrapy spider instance
        Return:
            The deferred
        '''
        # Run db query in the thread pool
        if self.run_id:
            d = self.dbpool.runInteraction(self._do_upsert, item, spider)
            # Any uncaught exceptions errback
            d.addErrback(self._handle_error, item, spider)
            # At the end return the item in case of either success or failure
            d.addBoth(lambda _: item)
            # return the deferred instead the item. This makes the engine to
            # process next item (according to CONCURRENT_ITEMS setting) after this
            # operation (deferred) has finished.
            return d
        else:
            raise CloseSpider('Exiting as no run id set.')

    # Overridable interface
    def _do_upsert(self, curs, item, spider):
        pass

    def _handle_error(self, failure, item, spider):
        '''
        Handle occurred on db interaction. Currently just logs the failure.

        Args:
            self --- The MYSQLWriter instance
            failure --- The Twisted failure instance (somewhat like an Exception)
            item --- The Scrapy item that we wish to write to database
            spider --- The Scrapy spider instance
        '''
        try:
            spider.logger.error('Exception was: \n%s. \n Item was:\n %s'
                                % (str(failure), item))
        except Exception:
            # If there is an issue logging the item, just log the failure
            spider.logger.error('Exception was: \n%s.' % str(failure))
