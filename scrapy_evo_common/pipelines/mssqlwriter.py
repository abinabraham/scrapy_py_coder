# The MSSQL Writer pipeline (not stored in pipelines.py because of the size)
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
from scrapy.exceptions import CloseSpider, NotConfigured
from scrapy.loader.processors import TakeFirst
from twisted.enterprise import adbapi
take_first = TakeFirst()


class BaseMSSQLWriter(object):
    """
    A pipeline to store the item in a MSSQL database.

    This implementation uses Twisted's asynchronous database API.
    (non blocking). If you use a blocking db writer, the whole spider
    will pause until the block clears (try with time.sleep to simulate really slow
    db)

    N.B. If you change from pyodbc to pymssql it will use different kwargs for conn
    and it also uses '%s' not '?' when you execute a command with params.

    N.B. `cp_max` is arg for adbapi telling it to use max of N conn in pool
        For booking.com (with heavy upsert interaction) anything > 1 crashes
        the database server and causes complete blocking (even though the
        upsert is ran in a sep thread, this blocking will eventually cause a
        pileup of items/responses in memory and Scrapy will stop processing
        new requests from the schedular until some items are processed. Thus
        all focus will turn to the blocking db thread until it clears. The
        thread remains blocked and doesn't output anything. This manifests
        itself as a hang, where nothing is happening, no db writes, no more
        crawling)

        `cp_reconnect` is an arg to tell it to reconnect automatically (default is False)
        This is an attempt to combat the disasters that happen when the connection
        is disturbed and we end up in "CAN'T COMMIT OR ROLLBACK" cycles until end
        of the scrape. Possibly a FreeTDS bug?

    Required settings:
        DB_TYPE  --- MYSQL or MSSQL
        DB_BOOKING_TABLE_DEFS --- Default table names for booking (master, room master,
                                  room conditions, prices, boxes, boxes list, merchant master)
        DB_AIRBNB_TABLE_DEFS ---- Default tables names for airbnb ( master, prices,
                                  boxes, boxes list, merchant mastre)
        DB_HOST  --- The db hostname
        DB_NAME  --- The db name
        DB_PORT  --- The db port
        DB_USER  --- The db user
        DB_PWD   --- The db password
    """

    def __init__(self, crawler_settings, crawler_stats):
        '''
        Init the MSSQLWriter instance. Make a connection to the database
        using the settings the user configured, and store some useful settings
        on the instance. If the user did not configre the required settings
        raise an excpetion

        Args:
            self --- The MSSQLWriter instance
            crawler_settings --- The crawler settings
        '''
        # Access to stats
        self.stats = crawler_stats
        try:
            # Image urls and paths fields
            self.images_urls_field = crawler_settings['IMAGES_URLS_FIELD']
            self.images_result_field = crawler_settings['IMAGES_RESULT_FIELD']
            # Dictionary specifying the connection arguments
            # If your connection string uses "SERVER=" then both freetds.conf and odbc.ini are ignored
            # If your connection string uses "SERVERNAME=" then the settings in
            # the appropriate freetds.conf server are used
            # If your connection string uses "DSN=" then the settings in the appropriate odbc.ini DSN are used
            # I found that with SERVER, explicitely stating TDS_Verion in the
            # pyodbc connection string fixed this (see notes in
            # conf/freetds.conf about which version, and not that below 7.0
            # unicode is not supported)
            # autocommit True because of deadlock errors booking
            # https://groups.google.com/forum/#!topic/web2py/lQ4ISVzIMzU
            dbargs = dict(
                TDS_Version=7.2,
                DRIVER='{FreeTDS}',
                SERVER=crawler_settings['DB_HOST'],
                PORT=crawler_settings['DB_PORT'],
                DATABASE=crawler_settings['DB_NAME'],
                UID=crawler_settings['DB_USER'],
                PWD=crawler_settings['DB_PWD'],
                autocommit=True
            )
            # Store the dbargs on the instance so we can access it in process_item
            self.dargs = dbargs
            # Connect to the database
            self.dbpool = adbapi.ConnectionPool('pyodbc', cp_max=1, cp_reconnect=1, **dbargs)
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
            cls --- The MSSQLWriter class
            crawler_settings --- The spider settings
        Returns:
            A configured instance of the MSSQLWriter class
        '''
        return cls(crawler.settings, crawler.stats)

    # @classmethod
    # def from_settings(cls, crawler_settings):
    #     '''
    #     See scrapy.middleware.py. If pipeline class has from_crawler
    #     or from settings construction with these is preferred.

    #     Args:
    #         cls --- The MSSQLWriter class
    #         crawler_settings --- The spider settings
    #     Returns:
    #         A configured instance of the MSSQLWriter class
    #     '''
    #     return cls(crawler_settings)

    # Overridable if need to set table names specific to different spiders
    def open_spider(self, spider):
        '''
        Just so we can read the table name on a per spider
        basis. If the spider instance doesn't have these names then we
        fall back to the defaults from settings.py

        Other attributes like merchant id and run id are also stored for
        future use. Note that these cannot be set during the init because they come
        from the spider, only available after spider opens.

        Args:
            self --- The MSSQLWriter instance
            spider --- The Scrapy spider instance
        '''
        try:
            # Set the marchant_id
            self.merchant_id = spider.merchant_id
            # Set the run_id
            self.run_id = spider.run_id
        except AttributeError:
            raise CloseSpider('Spider did not have either merchant id or run id attrib')

        # Set table names

    def close_spider(self, spider):
        """
        Cleanup function, called after crawing has finished to close
        db connection pool.
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
            self --- The MSSQLWriter instance
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
            # Return the deferred instead of the item. This makes the engine
            # process next item (according to `CONCURRENT_ITEMS` setting) after this
            # operation (deferred) has finished.
            return d
        else:
            raise CloseSpider('Exiting as no run_id set...')

    # Overridable interface
    def _do_upsert(self, curs, item, spider):
        pass

    def _handle_error(self, failure, item, spider):
        '''
        Handle occurred on db interaction. Currently just logs the failure.

        FUTURE:

        We may want to extend this. Occassionally we are getting server read/write
        errors (connection interrupted). Annoying FreeTDS does not handle this well at all,
        and we can end up with a "CAN'T COMMIT/ROLLBACK" pyodbc.Error that occurs
        for every subsequent interaction until the subscrape ends. We
        could potentially try restarting the pool here to limit the damage to one
        item?
            self.dbpool.close()
            self.dbpool = adbapi.ConnectionPool('pyodbc', cp_max=1, cp_reconnect=1, **self.dbargs)

        Args:
            self --- The MSSQLWriter instance
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
