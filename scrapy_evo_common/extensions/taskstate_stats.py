import sys
import pyodbc
import json
import os
import django
import logging
from _mysql_exceptions import OperationalError as MySQLOperationalError
from scrapy.exceptions import NotConfigured
from scrapy import signals
from scrapy.conf import settings as scrapy_settings
SCRAPY_BASE_DIR = scrapy_settings['PROJECT_ROOT']
DJANGO_DIR = os.path.join(SCRAPY_BASE_DIR, '../../../', 'evofrontend')
sys.path.insert(0, DJANGO_DIR)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", 'evofrontend.settings')
django.setup()
from evosched.models import SubScrapingTaskState
from django.core.exceptions import ObjectDoesNotExist, MultipleObjectsReturned


class SubScrapingTaskStateStats(object):
    '''
    Update the SubScrapingTaskState model
    for a given celery scraping run with the Scrapy stats. Also save the stats
    to the `scraper_stats` table of the MSSQL db assuming we are using it.

    User can set `WANTED_STATS_LIST` in settings.py to determine which statistics
    should be included.
    '''

    # Change labels to more human readable ones
    label_map = {'finish_reason': 'Exit status',
                 'downloader/request_count': 'Requests',
                 'item_scraped_count': 'Items scraped',
                 'item_dropped_count': 'Items dropped',
                 'downloader/response_status_count/200': 'Successful requests',
                 'downloader/response_bytes': 'Bytes received',
                 'dupefilter/filtered': 'Duplicates filtered',
                 'log_count/ERROR': 'Errors'}

    def __init__(self, stats, wanted_stats_list, db_args):
        '''
        Init the extension

        Args:
            self --- Extension instance
            stats --- The crawler stats
            wanted_stats_list --- List of stats that we wish to include
            db_args --- Dictionary specifying the MSSQL db connection settings
                        else None if we are not using MSSQL
        '''
        self.stats = stats
        self.wanted_stats_list = wanted_stats_list
        self.db_args = db_args

    @classmethod
    def from_crawler(cls, crawler):
        # Set the list of wanted stats from customisable settings
        wanted_stats_list = crawler.settings.getlist('WANTED_STATS_LIST', ['finish_reason',
                                                                           'item_scraped_count',
                                                                           'item_dropped_count',
                                                                           'log_count/ERROR',
                                                                           'dupefilter/filtered',
                                                                           'downloader/response_bytes',
                                                                           'downloader/request_count',
                                                                           'downloader/response_status_count/200'
                                                                           ])
        # Configure the db
        try:
            db_type = crawler.settings.get('DB_TYPE', None)
            dbargs = None
            if db_type == 'MSSQL':
                dbargs = dict(
                    DRIVER='{FreeTDS}',
                    SERVER=crawler.settings.get('DB_HOST'),
                    PORT=crawler.settings.get('DB_PORT'),
                    DATABASE=crawler.settings.get('DB_NAME'),
                    UID=crawler.settings.get('DB_USER'),
                    PWD=crawler.settings.get('DB_PWD'),
                )
        except KeyError:
            raise NotConfigured
        # Instantiate extension instance
        ext = cls(crawler.stats, wanted_stats_list, dbargs)
        # Connect the extension instance method to closed signal
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        # Return the extension instance
        return ext

    def spider_closed(self, spider):
        '''
        Method called when the spider is closed responsible for saving and formatting
        the stats, both on the stats field of the SubScrapingTaskState instance
        and the MSSQL db table `scraper_stats` (assuming we are using the production
        MSSQL db)

            > First get the subtask CELERY_TASK_ID from kwargs and use this to
              get the corresponding Django SubScrapingTaskState model
            > Filter the stats dictionary keeping only those wanted stats
            > Re-map the stats names to human-readable ones as determined by
              `label_map`
            > Save the stats dictionary as a JSON string on the stats field
              of the SubScrapingTaskState instance
            > Finally, if we are using the MSSQL production db, save the stats to the
              `scraper_stats` table.
        Args:
            self --- Instance of the extension
            spider --- A Scrapy spider instance
        '''

        # Determine which SubScrapingTaskState is being used for monitoring via the kwargs,
        try:
            celery_task_id = spider.CELERY_TASK_ID
        except AttributeError:
            spider.log('The spider has no CELERY_TASK_ID attrib...', level=logging.ERROR)
            return None

        # Get the corresponding SubScrapingTaskState
        try:
            self.task_state = SubScrapingTaskState.objects.get(celery_task_id=celery_task_id)
        except ObjectDoesNotExist:
            spider.log('No SubScrapingTaskState instance found with celery_task_id=%s.'
                       ' Cannot store stats...' % celery_task_id, level=logging.ERROR)
            return None
        except MultipleObjectsReturned:
            spider.log('More than one SubScrapingTaskState instance found with'
                       ' celery_task_id=%s. Cannot store stats...' % celery_task_id, level=logging.ERROR)
            return None
        except MySQLOperationalError as my_exc:
            spider.log('MySQL exception: %s' % my_exc, level=logging.ERROR)
            return None

        # Filter stats dict to just the wanted stats
        stats_dict = {k: v for (k, v) in self.stats.get_stats().items()
                      if k in self.wanted_stats_list}
        # Remap the stat names to human readable alternatives
        for k in self.label_map.keys():
            if k in stats_dict.keys():
                stats_dict[self.label_map[k]] = stats_dict.pop(k)

        # Save as JSON string and save on the SubScrapingTaskState instance
        self.task_state.stats = json.dumps(stats_dict)
        self.task_state.save()

        ########################################################################
        # Also save the stats on the MSSQL db(if not using MSSQL, self.db_args #
        # is None)                                                             #
        ########################################################################
        cnxn = None
        if hasattr(self, 'db_args') and self.db_args is not None:
            # spider.log('outer try: db_args is not None:%s' % self.db_args)
            try:
                # Get a cursor
                # dbargs = self.dbargs
                # spider.log('inner try: db_args is not None:%s' % db_args)
                cnxn = pyodbc.connect(**self.db_args)
                curs = cnxn.cursor()
                # Insert the stats row
                insert_cmd = """INSERT INTO scraper_stats (celery_task_uuid, pt_name, item_scraped_count,\
                                                           item_dropped_count, finish_reason, log_count_error,\
                                                           dupefilter_filtered, downloader_response_bytes, downloader_request_count,\
                                                           downloader_response_status_count_200)
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"""
                curs.execute(insert_cmd,  self.task_state.celery_task_id,
                             self.task_state.pt_name,
                             self.stats.get_value('item_scraped_count', 0),
                             self.stats.get_value('item_dropped_count', 0),
                             self.stats.get_value('finish_reason', None),
                             self.stats.get_value('log_count/ERROR', 0),
                             self.stats.get_value('dupefilter/filtered', 0),
                             self.stats.get_value('downloader/response_bytes', 0),
                             self.stats.get_value('downloader/request_count', 0),
                             self.stats.get_value('downloader/response_status_count/200', 0))
                curs.commit()
            except pyodbc.Error as pyodbc_err:
                spider.log('Could not connect to SQL server db to write stats.'
                           ' Error was: %s' % pyodbc_err, level=logging.ERROR)
            except Exception as exc:
                spider.log('Exception caught: %s' % exc, level=logging.ERROR)
            finally:
                # Cleanup
                if cnxn:
                    cnxn.close()
