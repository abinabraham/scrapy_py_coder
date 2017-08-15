from __future__ import division
from scrapy.extensions.statsmailer import StatsMailer
from scrapy.mail import MailSender
from scrapy.exceptions import NotConfigured
from scrapy import signals
import os


class ErrorStatsMailer(StatsMailer):
    '''
    Override extensions StatsMailer that sends email
    by catching spider closed signal, so that only certain
    stats are emailed and only upon error.

    Need to add this in EXTENSIONS under settings.py
    loading order is not so important for extensions
    EXTENSIONS = {
        'scrapy_evo_common.extensions.retail_errorstatsmailer.ErrorStatsMailer' : 500,
    }
    '''

    def __init__(self, stats, recipients, mail, test_server, use_feed_export):
        super(ErrorStatsMailer, self).__init__(stats, recipients, mail)
        self.test_server = test_server
        self.use_feed_export = use_feed_export

    @classmethod
    def from_crawler(cls, crawler):
        recipients = crawler.settings.getlist("STATSMAILER_RCPTS")
        if not recipients:
            raise NotConfigured
        mail = MailSender.from_settings(crawler.settings)
        test_server = crawler.settings.getbool("TEST_SERVER")
        use_feed_export = crawler.settings.getbool("USE_FEED_EXPORT")
        o = cls(crawler.stats, recipients, mail, test_server, use_feed_export)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

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

    def _exec_sql_check(self, spider, cnxn, cmd):
        '''
        Execute the user's custom SQL command and result
        the result if there are no exceptions, else
        return False by default
        '''
        if cnxn is not None:
            spider.logger.debug('Running custom SQL cmd %s' % cmd)
            curs = cnxn.cursor()
            try:
                # Replace the placeholder with the actual run id
                curs.execute(cmd.replace('<RUN_ID>', str(spider.RUN_ID)))
                res = curs.fetchone()
                if res is not None:
                    return res[0]
            except Exception as exc:
                spider.logger.error('Exception when running custom SQL check: %s'
                                    ' The exception was:\n %s' % (cmd.replace('<RUN_ID>', str(spider.RUN_ID)),
                                                                  exc))
        return False

    def spider_closed(self, spider):
        '''
        Method is called upon spider close. We get the stats, and
        we get some identifying attributes (celery uuid and celery worker name
        to tie this email to a given worker process). We send email upon certain
        triggers that the user configured during training the retail spider

        N.B. Another option is finish reason
        bad_finish = False
        reason = self.stats.get_value('finish_reason')
        if reason != 'finished':
            bad_finish = True

        Args:
            self --- Extension instance
            spider --- Scrapy spider instance
        '''
        spider.logger.debug('Starting retail_errorstatsmailer ErrorStatsMailer....')

        # Get the errmgmtconf
        errmgmtconf = spider.errormgmtconf

        # Add run id to stats
        self.stats.set_value('RUN_ID', spider.run_id)

        # Get the stats
        spider_stats = self.stats.get_stats(spider)
        requests_made = self.stats.get_value('downloader/request_count', 0)

        # Get the celery worker hostname from spider kwargs, else use machine host name
        try:
            hostname = spider.CELERY_WORKER_HOSTNAME
        except AttributeError:
            hostname = os.uname()[1]

        # Get the celery subtask UUID
        celery_task_id = None
        try:
            celery_task_id = spider.CELERY_TASK_ID
        except AttributeError:
            pass

        # Get connection so we can update the retail_runlog status if problem
        cnxn = self._get_cnxn(spider)

        # ################## Triggers ############################

        # Errors
        any_errors = False
        excsCount = 0
        for key in self.stats.get_stats(spider).keys():
            if 'spider_exceptions' in key:
                excsCount += self.stats.get_value(key)
            elif 'log_count/ERROR' in key:
                excsCount += self.stats.get_value(key)
        spider.logger.debug('Got %i exceptions' % excsCount)
        spider.logger.debug('The threshold is %s' % errmgmtconf.err_threshold_abs)
        if excsCount >= errmgmtconf.err_threshold_abs:
            spider.logger.debug('Setting any errors to True')
            any_errors = True

        # Any 503 responses
        banned_responses = False
        ban_resp_count = self.stats.get_value('downloader/response_status_count/503', 0)
        if errmgmtconf.banned_resp_abs_or_perc == 'P' and requests_made > 0:
            # Percentage threshold (if no requests then cannot be too many banned requests!)
            if (ban_resp_count/requests_made)*100 >= errmgmtconf.banned_resp_threshold:
                banned_responses = True
        elif errmgmtconf.banned_resp_abs_or_perc == 'A':
            # Absolute threshold
            if ban_resp_count >= errmgmtconf.banned_resp_threshold:
                banned_responses = True

        # Price items (rows inserted - custom stat) scraped too low
        too_few_prices = False
        if not self.use_feed_export:
            # items_scraped = self.stats.get_value('item_scraped_count', 0)
            price_row_count = self.stats.get_value('price_row_count', 0)
            if errmgmtconf.price_row_abs_or_perc == 'P':
                # Percentage threshold
                if requests_made > 0:
                    if ((price_row_count/requests_made)*100) <= errmgmtconf.price_row_threshold:
                        too_few_prices = True
                else:
                    # If no requests then def too few items too
                    too_few_prices = True
            elif errmgmtconf.price_row_abs_or_perc == 'A':
                # Absolute threshold
                if price_row_count <= errmgmtconf.price_row_threshold:
                    too_few_prices = True

        # SQL checks
        sql_check_results = []
        if not self.use_feed_export:
            for sql_check in spider.target_site.sqlchecks.all():
                sql_check_results.append((sql_check.cmd.replace('<RUN_ID>', spider.RUN_ID),
                                          self._exec_sql_check(spider, cnxn, sql_check.cmd)))

        # ################## Actions ############################
        # Build subject prefix
        globalsubject = '[Merchant: %s and Spider %s@%s]' % (spider.merchant_name, spider.name, hostname)
        # Build body postfix (stats and cel task id)
        globalbody = "\n\n%s stats for celery subtask id:%s\n\n" % (spider.name, celery_task_id)
        globalbody += "\n".join("%-50s : %s" % i for i in spider_stats.items())

        if any_errors and requests_made > 0:
            # Set the retail_run_log status
            if cnxn is not None:
                spider.logger.debug('Updating retail run log status to zero because exceptions')
                curs = cnxn.cursor()
                curs.execute("UPDATE retail_run_log SET status=0, [comment]=RTRIM(COALESCE([comment], '')) + ' %i errors' WHERE run_id=%s;"
                             % (excsCount, spider.RUN_ID))
                cnxn.commit()
            # Email
            error_recipients = [nemail.email_address for nemail in errmgmtconf.error_emails.all()]
            spider.logger.debug('Taking error action with email recips %s' % error_recipients)
            if error_recipients and not self.test_server:
                # Too many errors, take appropriate action by sending to relevant
                # emails and noting to log file potentially
                subject = globalsubject + ('There were %i exceptions, which is %.2f%% of requests'
                                           % (excsCount, ((excsCount/requests_made)*100)))
                body = ('You are receiving this email because this address is on an alert'
                        ' list for the trigger: scraping errors >= %i'
                        ' and the error count was %i\n%s'
                        % (errmgmtconf.err_threshold_abs, excsCount, globalbody))
                self.mail.send(error_recipients, subject, body)
            # Log to file?
            if errmgmtconf.err_log_to_file:
                spider.logger.warning('There were %i exceptions, which is %.2f%% of requests.'
                                      ' This >= the threshold of %s'
                                      % (excsCount, ((excsCount/requests_made)*100),
                                         errmgmtconf.err_threshold_abs))

        if banned_responses and requests_made > 0:
            # Set the retail_run_log status
            if cnxn is not None:
                spider.logger.debug('Updating retail run log status to zero because banned responses')
                curs = cnxn.cursor()
                curs.execute("UPDATE retail_run_log SET status=0, [comment]=RTRIM(COALESCE([comment], '')) + ' %i banned responses' WHERE run_id=%s;"
                             % (ban_resp_count, spider.RUN_ID))
                cnxn.commit()
            banned_resp_recipients = [nemail.email_address for nemail in errmgmtconf.banned_resp_emails.all()]
            if banned_resp_recipients and not self.test_server:
                subject = globalsubject + ('There were %i banned responses during scrape, which is %.2f%% of requests'
                                           % (ban_resp_count, ((ban_resp_count/requests_made)*100)))
                if errmgmtconf.banned_resp_abs_or_perc == 'P':
                    body = ('You are receiving this email because this address is on an alert'
                            ' list for the trigger: banned responses (503s) >= %s %%'
                            ' and the banned response percentage was %.2f%% \n%s'
                            % (errmgmtconf.banned_resp_threshold,  ((ban_resp_count/requests_made)*100),
                               globalbody))
                else:
                    body = ('You are receiving this email because this address is on an alert'
                            ' list for the trigger: banned responses (503s) >= %s'
                            ' and the banned response count was %s \n%s'
                            % (errmgmtconf.banned_resp_threshold, ban_resp_count,
                               globalbody))
                self.mail.send(banned_resp_recipients, subject, body)
            # Log to file?
            if errmgmtconf.banned_resp_log_to_file:
                ban_p_or_a = '%' if errmgmtconf.banned_resp_abs_or_perc == 'P' else '#'
                spider.logger.error('There were %i banned responses(503s), which is %.2f%% of requests.'
                                    ' This >= the threshold of %s (%s)'
                                    % (ban_resp_count, ((ban_resp_count/requests_made)*100),
                                       errmgmtconf.banned_resp_threshold, ban_p_or_a))
        # Always note the price_row_count in the price_row_count col for
        # convenience
        if cnxn is not None:
            spider.logger.debug('Updating retail run log price_row_count to value obtained')
            curs = cnxn.cursor()
            curs.execute("UPDATE retail_run_log SET price_row_count=%s WHERE run_id=%s;"
                         % (price_row_count, spider.RUN_ID))
            cnxn.commit()
        if too_few_prices and not self.use_feed_export:
            # Set the retail_run_log status
            if cnxn is not None:
                spider.logger.debug('Updating retail run log status to zero because too few prices')
                curs = cnxn.cursor()
                curs.execute("UPDATE retail_run_log SET status=0, [comment]=RTRIM(COALESCE([comment], '')) + ' only %i price rows' WHERE run_id=%s;"
                             % (price_row_count, spider.RUN_ID))
                cnxn.commit()
            price_row_recipients = [nemail.email_address for nemail in errmgmtconf.price_row_emails.all()]
            if price_row_recipients and not self.test_server:
                if requests_made > 0:
                    subject = globalsubject + ('There were %i price rows added during scrape,  which is %.2f%% of requests'
                                               % (price_row_count, ((price_row_count/requests_made)*100)))
                    if errmgmtconf.price_row_abs_or_perc == 'P':
                        body = ('You are receiving this email because this address is on an alert'
                                ' list for the trigger: price row count <= %s%%'
                                ' and the price row count percentage was %.2f%% \n%s'
                                % (errmgmtconf.price_row_threshold, ((price_row_count/requests_made)*100),
                                    globalbody))
                    else:
                        body = ('You are receiving this email because this address is on an alert'
                                ' list for the trigger: price row count <= %s'
                                ' and the price row count was %s \n%s'
                                % (errmgmtconf.price_row_threshold, price_row_count, globalbody))
                else:
                    subject = globalsubject + 'There were zero requests made during scrape'
                    body = ('You are receiving this email because this address is on an alert'
                            ' list for the trigger of low price row count'
                            ' and zero requests were made\n%s'
                            % globalbody)
                self.mail.send(price_row_recipients, subject, body)
            # Log to file?
            if errmgmtconf.price_row_log_to_file:
                if requests_made > 0:
                    prices_p_or_a = '%' if errmgmtconf.price_row_abs_or_perc == 'P' else '#'
                    spider.logger.error('There were %i price rows added, which is %.2f%% of requests.'
                                        ' This is <= the threshold of %s (%s)'
                                        % (price_row_count, ((price_row_count/requests_made)*100),
                                           errmgmtconf.price_row_threshold, prices_p_or_a))
                else:
                    spider.logger.error('No requests were made therefore no price rows added')

        # SQL checks
        if False in [res for (cmd, res) in sql_check_results] and not self.use_feed_export:
            comment = ' One or more custom SQL checks failed.'
            spider.logger.error(comment)
            if cnxn is not None:
                spider.logger.debug('Updating retail run log status to zero after SQL check fail')
                curs = cnxn.cursor()
                curs.execute("UPDATE retail_run_log SET status=0, [comment]=RTRIM(COALESCE([comment], '')) + ' %s' WHERE run_id=%s;" % (comment, spider.RUN_ID))
                cnxn.commit()
            sqlchecksfailed_recipients = [nemail.email_address for nemail in errmgmtconf.sqlchecksfailed_emails.all()]
            if sqlchecksfailed_recipients and not self.test_server:
                subject = globalsubject + (' One or more custom SQL checks failed.')
                body = ('You are receiving this email because this address is on an alert'
                        ' list for the trigger: SQL checks failed. \n%s'
                        % globalbody)
                self.mail.send(sqlchecksfailed_recipients, subject, body)
            # Log to file?
            if errmgmtconf.sqlchecksfailed_log_to_file:
                spider.logger.error('One or more custom SQL checks failed.')

        # Cleanup
        if cnxn is not None:
            cnxn.close()
