import sys
import datetime
import re
import pyodbc
import os
import django
from scrapy.exceptions import NotConfigured
from scrapy import signals
from scrapy.conf import settings as scrapy_settings
SCRAPY_BASE_DIR = scrapy_settings['PROJECT_ROOT']
DJANGO_DIR = os.path.join(SCRAPY_BASE_DIR, '../../../', 'evofrontend')
sys.path.insert(0, DJANGO_DIR)
os.environ.setdefault("DJANGO_SETTINGS_MODULE", 'evofrontend.settings')
django.setup()


class RetailLogCollector(object):
    '''
    Parse the logfile lines that have a datetime stamp between the start and end dates
    of the scrape. Write any error lines to a simple db table:
        run_id | stamp | level| msg

    This will enable easy browsing of the errors that occurred for a given scrape
    either via a frontend or a db client (e.g. by a QA)
    '''
    def __init__(self, stats, db_args, log_file, desired_levels):
        '''
        Init the extension

        Args:
            self --- Extension instance
            stats --- The crawler stats
            db_args --- Dictionary specifying the MSSQL db connection settings
                        else None if we are not using MSSQL
            log_file --- The log file name for this spider (absolute)
            desired_levels --- List of which log levels to collect, e.g. ["ERROR", "WARNING", ]
        '''
        self.stats = stats
        self.db_args = db_args
        self.log_file = log_file
        self.desired_levels = desired_levels

    @classmethod
    def from_crawler(cls, crawler):
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

        # These are the log levels that we want to collect on
        desired_levels = crawler.settings.get('LOG_COLLECTOR_LEVELS', ['ERROR'])
        # Log file
        log_file = crawler.settings.get('LOG_FILE')
        # Instantiate extension instance
        ext = cls(crawler.stats, dbargs, log_file, desired_levels)
        # Connect the extension instance method to closed signal
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        # Return the extension instance
        return ext

    def spider_closed(self, spider):
        '''
        Method called when the spider is closed responsible for parsing the log file
        lines and writing any errors out to db table
        Args:
            self --- Instance of the extension
            spider --- A Scrapy spider instance
        '''
        if not self.log_file:
            spider.logger.error('No log file attribute, so could not parse errors')
            return
        elif self.desired_levels is []:
            spider.logger.error('No log levels desired for collection. Do nothing.')
            return

        desired_lines = []
        rgx = re.compile("^(?P<stamp>(?:\d{4})-(?:\d{2})-(?:\d{2}) (?:\d{2}):(?:\d{2}):(?:\d{2})"
                         ") \[(?P<mod>[^\]]*?)\] (?P<level>[^\:]*?): (?P<msg>.*)")
        with open(self.log_file, 'r') as lf:
            lines = lf.readlines()
            for lineno, line in enumerate(lines):
                # Scrapy default log encoding is utf8
                decoded_line = line.decode('utf8')
                m = rgx.search(decoded_line)
                if m is not None:
                    cap_groups = m.groupdict()
                    # If the stamp is between the start and finish time for
                    # this run then append the error line
                    stamp = datetime.datetime.strptime(cap_groups.get('stamp'), '%Y-%m-%d %H:%M:%S')
                    level = cap_groups.get('level')
                    if self.stats.get_value('start_time') <= stamp <= self.stats.get_value('finish_time'):
                        if level in self.desired_levels:
                            # The user has specified which levels should be
                            # recored to db, so we collect only these
                            msg = cap_groups.get('msg', u'')
                            if level == u'ERROR':
                                # For error level, the subsequent lines may be a
                                # multiline Traceback that we'd also like to
                                # capture. Each line of Traceback is
                                # distinguished by not having the
                                # leading formatting.
                                # Capture all following lines until the next
                                # line with the <date> <mod> <level>: prefix
                                traceback_msg = u''
                                while True:
                                    lineno = lineno + 1
                                    try:
                                        nextline = lines[lineno]
                                    except IndexError:
                                        break
                                    nextm = rgx.search(nextline)
                                    if nextm is None:
                                        # Plain text line without leading stamp
                                        decoded_nextline = nextline.decode('utf8')
                                        traceback_msg += u'\n%s' % decoded_nextline
                                    else:
                                        # Line with formatting with we dont
                                        # want to append
                                        break
                                msg += traceback_msg
                            # Append to our desired lines
                            desired_lines.append((stamp, level, msg))

        spider.log(u'Got %i desired lines to collect to db collecting on levels: %s'
                   % (len(desired_lines), self.desired_levels))

        ########################################################################
        # Save the parsed lines to db(if not using MSSQL, self.db_args #
        # is None)                                                             #
        ########################################################################
        cnxn = None
        scope = '-'.join([spider.name, spider.MERCHANT_NAME])
        if hasattr(self, 'db_args') and self.db_args is not None:
            # spider.log('outer try: db_args is not None:%s' % self.db_args)
            try:
                # Get a cursor
                # dbargs = self.dbargs
                # spider.log('inner try: db_args is not None:%s' % db_args)
                cnxn = pyodbc.connect(**self.db_args)
                curs = cnxn.cursor()
                # Insert the stats row
                insert_cmd = """INSERT INTO retail_log_lines (run_id, scope, stamp, level, msg)
                                VALUES (?, ?, ?, ?, ?)"""
                for line in desired_lines:
                    curs.execute(insert_cmd,  (spider.RUN_ID, scope, line[0], line[1], line[2]))
                    curs.commit()
            except pyodbc.Error as pyodbc_err:
                spider.logger.error('Could not connect to SQL server db to write log lines.'
                                    ' Error was: %s' % pyodbc_err)
            except Exception as exc:
                spider.logger.error('Exception caught: %s' % exc)
            finally:
                # Cleanup
                if cnxn:
                    cnxn.close()
