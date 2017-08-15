# You can use this middleware to have a random user agent every request the spider makes.
from scrapy import signals
from scrapy.exceptions import NotConfigured, CloseSpider
import random
import pymongo


class RandomUserAgentMiddleware(object):
    '''
    This middleware choose a random user-agent per request. These
    user-agents are stored in a central mongodb (which is populated
    by running the getUserAgents crawler upon VM alloc).

    User-agent consistency is achieved with the 'User-Agent' key in the meta.
    That is to say, after setting the user-agent, we set the 'User-Agent' key in
    the meta to record that user agent, then in the spider callback the spider
    can check for that key in response.request.meta and set it again in the subsequent
    request's meta, then if a 'User-Agent' key is found in the request meta, this user-agent
    will be retained. This allows consistency of user-agents between say listing
    and detail pages. However if the user sets `RANDOM_USERAGENT_FORCE_ALWAYS_RENEW = True`
    then we will renew the user-agent with every single request.
    (Note that to achieve user-agent consistency the user still has to implement
    code in the spider callbacks to get the previous user-agent from the request
    and add it to the meta of the next request anyway)

    To enable this middleware, you will have to disable the default
    user-agent middleware and add this to your settings file.

    DOWNLOADER_MIDDLEWARES = {
         'scraper.random_user_agent.RandomUserAgentMiddleware': 400,
         'scrapy.contrib.downloadermiddleware.useragent.UserAgentMiddleware': None,
    }
    '''

    # Regex patterns to extract non-windows, widows or mobile user-agents
    keywords = {'windows_uas': ['windows', 'WinNT', 'Win95', 'Win98'],
                'mobile_uas': ['mobile', 'smartphone', 'pda', 'tablet', 'android',
                               'iphone', 'ipad', 'opera mini', 'blackberry', 'palm',
                               'nokia', 't-mobile', 'fennec', 'htc', 'phone', 'maemo', 'minimo',
                               'kindle', 'mobi', 'sonyrricsson', 'skyfire', 'teashark', 'teleca', 'uzard',
                               'windows ce', 'ppc', 'lynx', 'ibrowse', 'dolphin', 'bolt', 'samsung',
                               'doris', 'dorothy', 'gobrowser', 'iemobile', 'iris', 'mib']
                }

    def __init__(self, settings):
        '''
        Init the middleware by setting the attributes required to make a connection
        to the mongodb that stores the user-agents and also getting a list of spiders
        that need to use mobile user-agents for their requests.

        Args:
            self --- The downloader middleware instance
            settings --- The crawler settings
        '''
        # List of any spiders needing a mobile ua
        self.MOBILE_SPIDERS = settings.get('MOBILE_SPIDERS', [])
        self.debug_prints = settings.get('RANDOM_USERAGENT_DEBUG', False)
        self.force_always_renew = settings.get('RANDOM_USERAGENT_FORCE_ALWAYS_RENEW', False)
        # Mongo db which stores the user-agent strings we scraped
        try:
            self.mongo_uri = settings.get('MONGO_URI')
            self.mongo_db = settings.get('MONGO_UA_DATABASE')
            self.mongo_collection = settings.get('MONGO_UA_COLLECTION')
        except KeyError:
            raise NotConfigured

    @classmethod
    def from_crawler(cls, crawler):
        '''
        The main entry point to the crawler API. Instantiate
        the middlware class, passing the settings the init. Wire
        up the `spider_closed` and `spider_opened` methods to the corresponding signals.

        Args:
            cls --- The RandomUserAgentMiddleware class
            crawler --- The Scrapy crawler
        Returns:
            An instance of the RandomUserAgentMiddleware class
        '''
        ext = cls(crawler.settings)
        # Connect it up to spider opened and closed signals
        crawler.signals.connect(ext.spider_closed, signal=signals.spider_closed)
        crawler.signals.connect(ext.spider_opened, signal=signals.spider_opened)
        return ext

    def spider_opened(self, spider):
        '''
        This method is called when the spider opens. It makes a connection
        to the central mongodb and grabs user-agents of various types.

        Args:
            self --- The RandomUserAgentMiddleware instance
            spider --- The Scrapy spider instance
        '''
        # Init some vars
        self.fixed_useragent_string = ""
        self.db = None
        self.mobile_uas = None
        self.non_mobile_uas = None
        self.non_win_uas = None
        self.win_uas = None
        self.all_uas = None
        # Connect to mongo
        try:
            self.client = pymongo.MongoClient(self.mongo_uri)
        except Exception as exc:
            spider.logger.error('Could not connect to mongo db: \n%s' % str(exc))
            raise CloseSpider(str(exc))
        self.db = self.client[self.mongo_db]
        # Grab some user-agents categories (one time only)
        try:
            self.mobile_uas = self._get_ua_by_keywords(wanted_kw_selections=['mobile_uas', ])
            self.non_mobile_uas = self._get_ua_by_keywords(wanted_kw_selections=[],
                                                        unwanted_kw_selections=['mobile_uas', ])
            self.non_win_uas = self._get_ua_by_keywords(wanted_kw_selections=[],
                                                        unwanted_kw_selections=['windows_uas', 'mobile_uas'])
            self.win_uas = self._get_ua_by_keywords(wanted_kw_selections=['windows_uas', ],
                                                    unwanted_kw_selections=['mobile_uas', ])
            self.all_uas = self._get_ua_by_keywords()
        except Exception as exc:
            spider.logger.error('Exception when getting user agent by keyword: \n%s' % str(exc))
            raise CloseSpider(str(exc))
        spider.log('Got %i mobile agents, %i non-windows agents(and non-mob), '
                   ' %i windows agents(and non-mob), %i non-mob uas, %i total agents..'
                   % (len(self.mobile_uas), len(self.non_win_uas), len(self.win_uas),
                      len(self.non_mobile_uas), len(self.all_uas)))
        # In particular, Naked Wines seemed to fail on certain User-Agents
        if hasattr(spider, 'target_site') and spider.target_site.fixed_useragent_string:
            self.fixed_useragent_string = spider.target_site.fixed_useragent_string
            spider.log('Will use fixed user-agent %s instead of randomizing' %
                       self.fixed_useragent_string)
        # Close the spider if we got no user-agents
        if len(self.all_uas) == 0:
            raise CloseSpider('No user agent strings in mongo collection!'
                              ' You should run the getUserAgents spider to populate it...')

    def spider_closed(self, spider):
        '''
        This method is called when the spider closes. Tidy up by closing the connection
        to the mongodb

        Args:
            self --- The RandomUserAgentMiddleware instance
            spider --- The Scrapy spider instance
        '''
        self.client.close()

    def _get_ua_by_keywords(self, wanted_kw_selections=[], unwanted_kw_selections=[]):
        '''
        Returns user-agents from the mongo collection that
        contain case-insensitively keywords from the selected group, but not
        keywords from the unwanted lists

        The schema of the db is like
            {'browser_name': 'Camino 1.5.5',
             'user_agent_strings': [....]}

        Args:
            self --- The RandomUserAgentMiddleware instance
            wanted_kw_selections --- List of keywords keys from the self.keywords dict
                                    specifying what words the user-agent must include
            unwanted_kw_selection --- List of keyword keys from the self.keywords dict
                                    specifuing what words the user-agent must NOT include
        Returns:
            A list of user-agent strings with at least one wanted keyword and without
            any unwanted keywords
        '''
        # Return all of them
        if len(wanted_kw_selections) == 0 and len(unwanted_kw_selections) == 0:
            curs = self.db[self.mongo_collection].find({}, {"user_agent_strings": 1})
            return [ua for doc in curs for ua in doc['user_agent_strings']]
        else:
            wanted_keywords = [v for k, v in self.keywords.iteritems() if k in wanted_kw_selections]
            wanted_keywords = [item for sublist in wanted_keywords for item in sublist]  # Flatten
            unwanted_keywords = [v for k, v in self.keywords.iteritems() if k in unwanted_kw_selections]
            unwanted_keywords = [item for sublist in unwanted_keywords for item in sublist]  # Flatten
            # Project out the user_agent_strings field
            curs = self.db[self.mongo_collection].find({}, {"user_agent_strings": 1})

            if len(wanted_keywords) > 0 and len(unwanted_keywords) == 0:
                return [ua for doc in curs for ua in doc['user_agent_strings']
                        if any(keyword.lower() in ua.lower() for keyword in wanted_keywords)]
            elif len(wanted_keywords) == 0 and len(unwanted_keywords) > 0:
                # Flatten into a list of user agent strings ensuring we filter down by substr
                return [ua for doc in curs for ua in doc['user_agent_strings']
                        if not any(unwanted_keyword.lower() in ua.lower() for unwanted_keyword in unwanted_keywords)]
            elif len(wanted_keywords) > 0 and len(unwanted_keywords) > 0:
                # Flatten into a list of user agent strings ensuring we filter down by substr
                return [ua for doc in curs for ua in doc['user_agent_strings']
                        if any(keyword.lower() in ua.lower() for keyword in wanted_keywords)
                        and not any(unwanted_keyword.lower() in ua.lower() for unwanted_keyword in unwanted_keywords)]
        return []

    def process_request(self, request, spider):
        '''
        Process the request and set a random user-agent in the header.

        We choose the agent from the pool depending on the spider. If the spider
        is in the list of those that require mobile user-agents we choose mobile. If
        the spider is airbnb and this is an image request, we choose non-windows user-agent
        otherwise we occassionally get formats, like JPEG-XR, that the linux server
        doesn't like. Otherwise we just choose from full pool.
        We set the 'User-Agent' key in the meta to record the choice, so that
        the callback that deals with the response can access it, set the key
        again in the meta of subsequent requests, and allowing to achieve user-agent conistency
        (e.g. between listing and detail)

        If the 'User-Agent' is already set in the meta, we use that user-agent again.

        Args:
            self --- The instance of the middleware
            request --- The Scrapy http Request instance being processed
            spider --- The Scrapy spider
        '''
        # If the user opted for a fixed user-agent then use it always
        if self.fixed_useragent_string and not request.meta.get('User-Agent', False):
            request.meta['User-Agent'] = self.fixed_useragent_string
            if self.debug_prints:
                spider.logger.debug('Set fixed user-agent: %s' % self.fixed_useragent_string)

        if not request.meta.get('User-Agent', False) or self.force_always_renew:
            if spider.name in self.MOBILE_SPIDERS:
                ua = random.choice(self.mobile_uas)
            elif spider.name == 'Airbnb' and request.meta.get('IMAGES_PIPELINE', False):
                # Otherwise we sometimes get JPEG-XR (windows specific fmt)
                ua = random.choice(self.non_win_uas)
            else:
                ua = random.choice(self.non_mobile_uas)
            # Now set the meta for next time
            request.meta['User-Agent'] = ua
            if self.debug_prints:
                spider.logger.debug('No meta User-Agent key  or always renew on for request.url: %s, so set randomly as %s' % (request.url, ua))
        else:
            # In order to keep session consistency we use the same user-agent
            # that was used to initially request the given listing page.
            # The callback can access this meta and reset the same meta
            # User-Agent key on the subsequent request in the chain, so we keep
            # the same ua within each listing page ->detail->xhr chain.
            ua = request.meta.get('User-Agent')
            if self.debug_prints:
                spider.logger.debug('Already meta User-Agent key and not always renew for request.url: %s, so keep this one: %s' % (request.url, ua))

        # Set the user-agent header
        request.headers.setdefault('User-Agent', ua)
