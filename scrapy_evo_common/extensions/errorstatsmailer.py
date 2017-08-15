from __future__ import division
from scrapy.extensions.statsmailer import StatsMailer
import os


class ErrorStatsMailer(StatsMailer):
    '''
    Override extensions StatsMailer that sends email
    by catching spider closed signal, so that only certain
    stats are emailed and only upon error.

    Need to add this in EXTENSIONS under settings.py
    loading order is not so important for extensions
    EXTENSIONS = {
        'scrapy_evo_common.extensions.errorstatsmailer.ErrorStatsMailer' : 500,
    }
    '''

    # Threshold percentages for triggers
    items_threshold_percentage = 10
    banned_response_threshold_percentage = 1
    exc_threshold_percentage = 1

    def spider_closed(self, spider):
        '''
        Method is called upon spider close. We get the stats, and
        we get some identifying attributes (celery uuid and celery worker name
        to tie this email to a given worker process). We send email upon certain
        triggers.

        Args:
            self --- Extension instance
            spider --- Scrapy spider instance
        '''
        # Get the stats
        spider_stats = self.stats.get_stats(spider)

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

        # ################## Reasons to send email ############################

        # Items scraped too low
        too_few_items = False
        items_scraped = self.stats.get_value('item_scraped_count', 0)
        requests_made = self.stats.get_value('downloader/request_count', 0)
        if requests_made > 0:
            if (items_scraped/requests_made)*100 <= self.items_threshold_percentage:
                too_few_items = True

        # Any 503 responses
        banned_responses = False
        ban_resp_count = self.stats.get_value('downloader/response_status_count/503', 0)
        if requests_made > 0:
            if (ban_resp_count/requests_made) >= self.banned_response_threshold_percentage:
                banned_responses = True

        # Finished for some reason other than 'finished'
        bad_finish = False
        reason = self.stats.get_value('finish_reason')
        if reason != 'finished':
            bad_finish = True

        # Errors
        any_errors = False
        excsCount = 0
        for key in self.stats.get_stats(spider).keys():
            if 'spider_exceptions' in key:
                excsCount += self.stats.get_value(key)
        if requests_made > 0 and (excsCount/requests_made) >= self.exc_threshold_percentage:
            any_errors = True

        if (too_few_items or banned_responses or bad_finish or any_errors):
            # Send email if trigger met

            # Build subject
            subject = '[Spider %s@%s]' % (spider.name, hostname)
            if too_few_items:
                subject += 'Only %i items were scraped, which is %.2f%% of requests' % (items_scraped, (items_scraped/requests_made)*100)
            elif banned_responses:
                subject += 'There were %i banned responses during scrape, which is %.2f%% of requests' % (ban_resp_count, ((ban_resp_count/requests_made)*100))
            elif bad_finish:
                subject += 'The finish reason was %s' % reason
            elif any_errors:
                subject += 'There were %i exceptions,  which is %.2f%% of requests' % (excsCount, ((excsCount/requests_made)*100))

            # Build body
            body = "\n\n%s stats for celery subtask id:%s\n\n" % (spider.name, celery_task_id)
            body += "\n".join("%-50s : %s" % i for i in spider_stats.items())

            # Send mail
            return self.mail.send(self.recipients, subject, body)
        else:
            pass
