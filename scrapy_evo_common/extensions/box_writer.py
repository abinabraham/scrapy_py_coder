from scrapy import signals
from scrapy.exceptions import NotConfigured
from pymongo import MongoClient
import logging
log = logging.getLogger(__name__)


class BoxWriter(object):
    '''
    After scraping, stores the replacement boxes in a central mongodb.
    This mongodb is an intermediary, and only once all subtask scrapes have finished
    from our distributed crawl (from different workers and/or nodes),
    will this mongodb be analyzed and used to make replacements
    in the production db by the `box_analyzer` callback celery subtask
    '''

    def __init__(self, settings):
        # Mongo db which stores the boxes strings we scraped
        try:
            self.mongo_uri = settings.get('MONGO_URI')
            self.mongo_db = settings.get('MONGO_BOXES_DATABASE')
            self.mongo_collection = settings.get('MONGO_BOXES_COLLECTION')
        except KeyError:
            raise NotConfigured

    @classmethod
    def from_crawler(cls, crawler):
        ext = cls(crawler.settings)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        return ext

    def spider_opened(self, spider):
        # Connect to Mongo
        self.client = MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def storeBoxes(self, spider, box, replacementBoxes):
        """
        This method takes a box obj and a list of replacement child boxes
        and stores them in central mongodb.

        The `run_id` is tracked so that we organize collections by run.
        The parent box tuple is used to index a list of replacements for it.
        Various subtasks may offer up different replacement meshes for this parent
        (since they scape over different date ranges, potentially leading to different
         granularity of mesh needed for that parent), so the subtask UUID is also stored
         with the replacments.
        The task responsible for actually doing the replacement at the end of the parallel
        subscrapes will do the replacement for the parent using the finest mesh (subscrape
        that offered the most replacements or finest mesh)

        Args:
            self --- The extension instance
            spider --- Scrapy spider
            box --- The parent box. A Box object instance
            replacementBoxes --- A list of replacmenet boxes. List of Box object
                                 instances
        """
        replacements = {'celery_task_uuid': spider.CELERY_TASK_ID,
                        'replacement_boxes': [{'sw_lat': rb.sw_lat, 'sw_lng': rb.sw_lng,
                                              'ne_lat': rb.ne_lat, 'ne_lng': rb.ne_lng}
                                              for rb in replacementBoxes]
                        }
        self.db[self.mongo_collection].update({'run_id': spider.RUN_ID,
                                               'parent_box': {'sw_lat': box.sw_lat, 'sw_lng': box.sw_lng,
                                                              'ne_lat': box.ne_lat, 'ne_lng': box.ne_lng}
                                               },
                                              {'$push':  {'replacements': replacements}},
                                              upsert=True
                                              )

    def spider_closed(self, spider):
        '''
        This is called upon the spider close signal. It loops over the original (parent)
        boxes (list of Box object instances) that made the original mesh at the start of the run,
        and for each parent box it analyses the replacement set of boxes (recorded in the descendent
        children tree of the parent Box instances), over all checkin dates, deciding which
        set is the finest (contains the most boxes). It nominates that set as the replacement
        for the given parent box.

        N.B. `spider.boxes` is a dictionary keyed by box ids with parent Box
        object instances as values. The parent Box instances don't have checkins
        associated but the children linked do.

        Args:
            self --- The extension instance
            spider --- Scrapy spider instance
        '''
        try:
            # spider.boxes is a dictionary keyed by box id
            for pBox in spider.boxes.values():
                # If no checkins, no replacements either...
                checkins = {child.checkin for child in pBox.children}
                if checkins:
                    # Now we want to find the finest mesh over all checkins
                    checkinsCount = [(checkin, len(pBox.getLeafDescendants(checkin))) for checkin in checkins]
                    # Reverse sort them (highest count at start) and get the checkin with finest mesh (i.e. most leaf desc)
                    finestCheckin = sorted(checkinsCount, key=lambda x: -x[1])[0][0]
                    # Now we want to replace pBox with the mesh from this checkin date
                    replacementBoxes = pBox.getLeafDescendants(finestCheckin)
                    log.debug('For uuid: %s. The parent box: %s will be replaced by %i boxes from checkin %s'
                              % (spider.CELERY_TASK_ID, pBox, len(replacementBoxes), finestCheckin))
                    self.storeBoxes(spider, pBox, replacementBoxes)
        except Exception as exc:
            raise exc
        finally:
            # Cleanup and close mongo db even if exceptions
            self.client.close()
