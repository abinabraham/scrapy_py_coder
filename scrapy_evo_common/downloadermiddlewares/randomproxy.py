from scrapy.exceptions import NotConfigured  # IgnoreRequest  # , CloseSpider
from scrapy import signals
from pymongo import MongoClient
from datetime import datetime, timedelta
from twisted.internet import defer
from twisted.internet.error import TimeoutError, \
    ConnectionRefusedError, ConnectionDone, ConnectError, \
    ConnectionLost, TCPTimedOutError
from scrapy.xlib.tx import ResponseFailed
import random
import time
import base64
import logging
log = logging.getLogger(__name__)


class ProxyPoolExhausted(Exception):
    pass


class RandomProxyBase(object):
    '''
    Some common methods for RandomProxy and RandomProxyDecrConnCount mw.
    '''

    PROXY_FAIL_EXCEPTIONS = (defer.TimeoutError, TimeoutError,
                             ConnectionRefusedError, ConnectionDone, ConnectError,
                             ConnectionLost, TCPTimedOutError, ResponseFailed,
                             )

    def __init__(self, settings):
        '''
        Store the mongo connection params and make connection.
        Also store other settings such as country, whether to blacklist, the blacklist
        duration, if capping is active and the connection cap.

        Determine if we should blacklist failed proxies from settings. E.g. with
        a proxy cloud service (like proxymesh), they rotate
        IPs for a given endpoint on their end (and those IPs change daily),
        we can randomly select an endpoint using this middleware, but we don't
        want to remove that endpoint if an exception occurs, since that endpoint
        represents many different proxy IPs.

        Determine also the blacklist duration, and the cap on the number of simultaneous
        connections any single proxy should get if capping is active, and the maximum fails allowed before
        we blacklist

        Args:
            self --- Instance of the middleware class
            settings --- The crawler settings
        '''

        # Mongo db which stores the boxes strings we scraped
        try:
            self.mongo_uri = settings.get('MONGO_URI')
            self.mongo_db = settings.get('MONGO_PROXY_DATABASE')
            self.mongo_collection = settings.get('MONGO_PROXY_COLLECTION')
            if not self.mongo_uri or not self.mongo_db or not self.mongo_collection:
                # NB this exception doesn't kill the spider, just disabled the
                # middleware
                raise NotConfigured('MONGO_ settings are required')
            # Make the connection
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
            self.collection = self.db[self.mongo_collection]
        except KeyError:
            # Just disables middleware
            raise NotConfigured

        # From which country do we want proxies to be located?
        self.country_name = settings.get('COUNTRY_NAME', 'UK')

        # Are we using cloud proxy that rotates at their end we prob don't want
        # to blacklist the entire endpoint
        self.dont_blacklist = settings.get('PROXY_DONT_BLACKLIST', False)

        # How many fails before we blacklist a proxy temporarily
        self.MAX_FAILS = settings.get('PROXY_MAX_FAILS', 5)

        # How long to blacklist for? (15 mins if not set)
        self.black_list_duration = settings.get('PROXY_BLACKLIST_DURATION', 900)
        # Limit the number of simultaneous requests via a given proxy
        self.connection_cap = settings.get('PROXY_CONNECTION_CAP', 1)
        self.capping_active = settings.get('PROXY_CAPPING_ACTIVE', False)

    @classmethod
    def from_crawler(cls, crawler):
        mw = cls(crawler.settings)
        crawler.signals.connect(mw.spider_closed, signal=signals.spider_closed)
        return mw

    def spider_closed(self, spider, reason):
        # close mongo db
        self.client.close()

    def update_conn_count(self, proxy_address, increase=True, debug=False):
        '''
        Update the mongodb by increasing or decreasing the CONNECTION_COUNT
        for this proxy

        Args:
            proxy_address --- The proxy to be updated
            increase (bool) --- Should we inc/dec (by +-1)
            debug(bool) --- Get the post update CONNECTION_COUNT and log it
                            if debug is True
        Returns:
            None
        '''
        # Increase or decrease?
        if increase is True:
            delta = 1
        else:
            delta = -1
        # Do the update
        try:
            res = self.collection.update_one({'PROXIES.ADDRESS': proxy_address},
                                             {'$inc': {'PROXIES.$.CONNECTION_COUNT': delta}}
                                             )
            if res.modified_count == 1:
                if debug is True:
                    # ########################## DEBUG #########################
                    # Get CONNECTION_COUNT for this proxy
                    curs = self.collection.aggregate([{"$match": {"COUNTRY_NAME": self.country_name}},
                                                      {"$project": {"PROXIES": 1}},
                                                      {"$unwind": "$PROXIES"},
                                                      {"$match": {"PROXIES.ADDRESS": proxy_address}},
                                                      ])
                    docs = [doc for doc in curs]
                    connection_count = docs[0]['PROXIES'].get('CONNECTION_COUNT', 0) if len(docs) > 0 else 0
                    log.debug('Changed CONNECTION_COUNT by delta %s. Now it is %s for proxy <%s>'
                              % (delta, connection_count, proxy_address))
                    # ##########################################################
            else:
                log.error('Could not update CONNECTION_COUNT for proxy <%s> by %s'
                          % (proxy_address, delta))
        except Exception as exc:
            log.error('Exception occurred when updating CONNECTION_COUNT: %s' % exc)


class RandomProxy(RandomProxyBase):
    '''
    Choose a random proxy from the pool per request.

    The proxies mongodb documents have the following structure:
    (IP is OK instead of hostname but don't forget scheme)
        {'COUNTRY_NAME': <country_name>,
         'PROXIES': [{'PROXY_ADDRESS': <scheme://hostname:port>,
                      'USER': <username>,
                      'PASS': <password>,
                      'BLACKLISTED': <ISODate("%Y-%m-%dT%H:%M:%S.%fZ") or False>,
                      'FAIL_COUNT': 0,
                      'CONNECTION_COUNT': 0
                      },
                      .
                      .
                      .
                     ]
         }

    We use the `COUNTRY_NAME` setting to determine which pool of proxies to choose from.
    The `COUNTRY_NAME` setting is set in `evosched/tasks.py` according to the
    `COUNTRY_ID` kwarg the user sets. Else we take 'UK' as default.

    Proxies that fail get blacklisted (i.e. their 'BLACKLISTED' attribute is set to a
    a datetime in the future, so they are not tried again until then).
    They are no longer used when selecting the initial pool to choose from.

    If capping is active, the connection count is tracked for the proxy under `CONNECTION_COUNT`;
    The incr/decr is done by RandomProxyConnCount (see notes in that middleware for why)
    In theory, it should limit the threads per proxy to `PROXY_CONNECTION_CAP` (default 1)
    settings.

    This mw works alongside the Scrapy `HttpProxyMiddleware` middleware (which looks for
    `proxy` in the request meta. HttpProxyMiddleware should be
    called after `RandomProxy` in `DOWNLOADER_MIDDLEWARES` of settings.py, e.g.
    DOWNLOADER_MIDDLEWARES = {'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
                              'scrapy_evo_common.downloadermiddlewares.customretry.CustomRetryMiddleware': 90,
                              'scrapy_evo_common.downloadermiddlewares.randomproxy.RandomProxy': 100,
                              'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
                              'scrapy_evo_common.downloadermiddlewares.randomproxy.RandomProxyConnCount': 960
                              }
    and the `CustomRetryMiddleware` should also be used because the instability of proxies (it differs only from default
    because it retries on proxypoolexhausted custom exception too)
    It's important to turn off the built-in retry middleware, don't just omit it. Also
    It's important that retry downloader middleware occurs lower than proxy middleware.
    The order of `process_request` is from lower to higher middeware, but the order
    of process_exception and process_response is reversed, i.e. it goes higher to lower,
    and the RandomProxy.process_exception occurs before CustomRetryMiddleware.process_exception.
    The `RandomProxyConnCount` mw, must go last to ensure decr always happens.

    N.B.
    The default `DOWNLOAD_TIMEOUT = 180`, this is quite long, so we could lower it.
    By default the retry middleware will repeat the request RETRY_TIMES at the end
    of the scrape (this is why the spider will appear to halt; there is no other work for the spider
    to do. It just waits for the retries). The retry middleware should appear first in
    middlewares so that random proxy is chosen after it, for a different proxy upon each attempt.
    If already a proxy in meta, `process_request` then checks the retry number for this request
    and forces the same proxy again, but then on subsequent attempts a new proxy is selected.
    '''

    def process_request(self, request, spider):
        '''
        Process the request.

        The HttpProxyMiddleware uses the meta key "proxy" to determine
        which proxy to direct requests through, we simply choose a proxy
        from our database at random then set that meta key for it to use.
        Along with the 'Proxy-Authorization' header.

        The proxy middleware by default shouldn't be used for the PUT requests to s3/azure
        (these requests are not via Scrapy http Request objs, and won't be subjected
        to downloader middlewares. I verified with wireshark the direction connection
        to Azure IP is being made.)

        We select only non-blacklisted proxies and proxies with a connection count
        below the cap (if capping active) of the country of interest. This the pool we choose randomly from.

        Args:
            self --- The middleware class instance
            request --- The Scrapy http Request instance being processed
            spider --- The Scrapy spider instance

        Exceptional behaviour:
            If there are no more alive proxies, the ProxyPoolExhausted exception
            is raised, which will propagate backwards along the `process_exception`
            chain (later middelwares first --- meaning random proxy before retry)
        '''
        # Retry requests
        orig_proxy_address = request.meta.get('proxy', None)
        if orig_proxy_address and request.meta.get('retry_times', 0) <= 1:
            # This will mean the retry middleware will try the same proxy once
            # then randomly select a new one.
            return None

        # Get proxies for the specified country. Aggregate performs a series of
        # actions in sequence. First get just the doc matching the country name.
        # Project out just the proxies field.
        # Next "unwind" deconstructs the PROXIES array field to output a document for each element
        # of the array. Then we can filter these documents again, finding just
        # the proxies with CONNECTION_COUNT below the cap (if capping is
        # active) AND non-blacklist proxies.
        # Non-blacklisted proxies are either with BLACKLISTED False (never blacklisted) or
        # BLACKLISTED until an earlier datetime than now (blacklist expired)
        utcnow = datetime.utcnow()
        if self.capping_active:
            curs = self.collection.aggregate([{"$match": {"COUNTRY_NAME": self.country_name}},
                                              {"$project": {"PROXIES": 1}},
                                              {"$unwind": "$PROXIES"},
                                              {"$match": {"$and": [{"PROXIES.CONNECTION_COUNT": {"$lt": self.connection_cap}},
                                                                   {"$or": [{"PROXIES.BLACKLISTED": {"$lte": utcnow}},
                                                                            {"PROXIES.BLACKLISTED": False}]
                                                                    },
                                                                   {"PROXIES.ADDRESS": {"$ne": orig_proxy_address}}
                                                                   ]
                                                          }
                                               }
                                              ])
        else:
            curs = self.collection.aggregate([{"$match": {"COUNTRY_NAME": self.country_name}},
                                              {"$project": {"PROXIES": 1}},
                                              {"$unwind": "$PROXIES"},
                                              {"$match": {"$and": [{"$or": [{"PROXIES.BLACKLISTED": {"$lte": utcnow}},
                                                                            {"PROXIES.BLACKLISTED": False}]
                                                                    },
                                                                   {"PROXIES.ADDRESS": {"$ne": orig_proxy_address}}
                                                                   ]
                                                          }
                                               }
                                              ])
        alive_proxies = [doc['PROXIES'] for doc in curs]

        # Check if we still have some proxies left
        if(len(alive_proxies)) > 0:
            proxy_choice = random.choice(alive_proxies)
            proxy_user_pass = None
            if proxy_choice['USER'] and proxy_choice['PASS']:
                proxy_user_pass = proxy_choice['USER'] + ':' + proxy_choice['PASS']
            request.meta['proxy'] = str(proxy_choice['ADDRESS'])
            if proxy_user_pass:
                # mimic downloadermiddlewares.httpproxy
                basic_auth = 'Basic ' + base64.b64encode(proxy_user_pass).strip()
                request.headers['Proxy-Authorization'] = basic_auth
            log.debug('Using proxy address: %s when getting URL:%s'
                      % (proxy_choice['ADDRESS'], request.url))
        else:
            # The custom retry middleware should catch this and then retry the
            # request (first it will be processed by self.process_exception
            # before it propagates along to other mw...because we return None)
            raise ProxyPoolExhausted('Proxy pool is exhausted...')

    def process_exception(self, request, exception, spider):
        '''
        Errback

        If the pool is exhausted momentarily block then propagate the exception
        to retry middleware, which will retry if the `retry_times` count has been
        exceeded.


        If the exception is one that we deem to be a potential proxy failure, then
        increment the FAIL_COUNT of the proxy in the db, and if that FAIL_COUNT
        is exceeded the maximum failure count then blacklist the proxy until a given
        datetime, unless the user has explicitley told us not to blacklist proxies
        (this can sometimes be needed for cloud proxy services that rotate at their
        end but just have a single entry endpoint)

        :: NOTES ::

        > The count decr is done by the RandomProxyDecrConnCount mw(if capping active)
          not this mw

        > With IgnoreRequest, like any exception it propogates down the process_exception
        chain and if nothing handles it then the errback of the Request itself handles it
        (if there is one), however if still unhandled the Request is just ignored
        without even being logged so be careful with this, unless you really do want to ignore it.

        > Returning None the process exception chain continues...

        > The process_request chain occurs in order of the downloader
        middleware, but the process_response/process_exception chains happen in
        reverse (i.e. the later middleware process responses and exceptions first).
        This means random proxy process exception is called BEFORE retry process
        exception. !IT IS VERY IMPORTANT THE RETRY MW COMES BEFORE PROXY MW. Otherwise
        retry mw handles the exception first and can fire off a new request halting
        the process exception chain before it ever reaches here, and critically
        before we get chanace to decrement proxy connection count!

        Args:
            self --- The middleware class instance
            request --- The Scrapy http Request instance
            exception --- The exception that occurred
            spider --- The Scrapy spider instance
        Returns:
            None --- We always propagate along the process exception chain
                     (which means the next lower middleware process_exception
                      --- retry mw --- is called next)
        '''
        # If proxy pool exhausted delay before propagating to retry downloader
        # middleware to give a chance for the pool to recover
        if type(exception) == ProxyPoolExhausted:
            time.sleep(1)
            return None

        if isinstance(exception, self.PROXY_FAIL_EXCEPTIONS):
            # We record the fail in the db
            res = self.collection.update_one({'PROXIES.ADDRESS': proxy_address},
                                             {'$inc': {'PROXIES.$.FAIL_COUNT': 1}})
            if res.modified_count == 1:
                log.error('Recorded fail for proxy <%s>. Requesting URL:%s'
                          '\n The exception was:\n%s'
                          % (proxy_address, request.url, repr(exception)))
            else:
                log.error('Could not record fail for proxy <%s>' % proxy_address)
                return None

            # Get the fail count for the proxy
            curs = self.collection.aggregate([{"$match": {"COUNTRY_NAME": self.country_name}},
                                              {"$project": {"PROXIES": 1}},
                                              {"$unwind": "$PROXIES"},
                                              {"$match": {"PROXIES.ADDRESS": proxy_address}},
                                              ])
            docs = [doc for doc in curs]
            fail_count = docs[0]['PROXIES'].get('FAIL_COUNT', 0) if len(docs) > 0 else 0

            # Determine whether to blacklist based on fail count and user settings
            if not self.dont_blacklist and fail_count >= self.MAX_FAILS:
                # Match proxy with this address and update BLACKLISTED field to future datetime
                # (The query part will match the full doc with an element in PROXIES array with matching address
                # The $ stands for first object in the array that matches query condition)
                expiration_date = datetime.utcnow() + timedelta(seconds=self.black_list_duration)
                res = self.collection.update_one({'PROXIES.ADDRESS': proxy_address},
                                                 {'$set': {'PROXIES.$.BLACKLISTED': expiration_date}
                                                  })
                if res.modified_count == 1:
                    log.error('Blacklisted failed proxy <%s> until %s. Retry times: %s. Fail count: %s'
                              ' Requesting URL:%s \n The exception was:\n%s'
                              % (proxy_address, expiration_date, request.meta.get('retry_times', 0),
                                 fail_count, request.url, repr(exception)))
                else:
                    log.error('Failed to blacklist proxy <%s>' % proxy_address)

        # Propagate along `process_exception` chain --- retry mw next down
        return None


class RandomProxyConnCount(RandomProxyBase):
    '''
    Ensure the connection count for the proxy is incremented/decrecemented.
    The reason for having this as separate middleware to RandomProxy is so
    that we can put it last in the middlware chain (even after built-ins) which ensures that process_response/process_exception
    of this middleware is always called, and count is always decremented. By experiments with memcached
    count middleware, we determined that if earlier in the chain other middleware can often interupt the response
    chain (e.g. if it returns a Request rather than Response object:
    https://doc.scrapy.org/en/latest/topics/downloader-middleware.html#scrapy.downloadermiddlewares.DownloaderMiddleware.process_response)
    This way stops the count running away to ever increasing values.

    In practice we may not need to keep counts on the proxies at all, just let
    random selection of the proxies take care of distributing load to each proxy. We
    make it optional via `capping_active`.


    The proxies mongodb documents have the following structure:
    (IP is OK instead of hostname but don't forget scheme)
        {'COUNTRY_NAME': <country_name>,
         'PROXIES': [{'PROXY_ADDRESS': <scheme://hostname:port>,
                      'USER': <username>,
                      'PASS': <password>,
                      'BLACKLISTED': <ISODate("%Y-%m-%dT%H:%M:%S.%fZ") or False>
                      'CONNECTION_COUNT': 0
                      },
                      .
                      .
                      .
                     ]
         }

    Should be added at the end of the middleware chain (even after built-ins) so
    process_response/processe_exception get called first and are guaranteed to be executed

    DOWNLOADER_MIDDLEWARES = {'scrapy.downloadermiddlewares.retry.RetryMiddleware': None,
                              'scrapy_evo_common.downloadermiddlewares.customretry.CustomRetryMiddleware': 90,
                              'scrapy_evo_common.downloadermiddlewares.randomproxy.RandomProxy': 100,
                              'scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware': 110,
                              'scrapy_evo_common.downloadermiddlewares.randomproxy.RandomProxyDecrConnCount': 960
                              }
    '''

    def process_request(self, request, spider):
        '''
        Increment the count for the proxy chosen for the request

        Args:
            self --- The middleware class instance
            request --- The Scrapy http Request instance being processed
            spider --- The Scrapy spider instance

        '''
        # Get the proxy we are using
        proxy_address = request.meta.get('proxy', None)
        if proxy_address and self.capping_active:
            # Incremement the proxy's CONNECTION_COUNT in the db
            self.update_conn_count(proxy_address, increase=True, debug=True)

    def process_response(self, request, response, spider):
        '''
        Finished using proxy, decrement the `CONNECTION_COUNT` of proxy in the db

        Args:
            self --- The middleware class instance
            request --- The Scrapy http Request instance being processed
            response --- The Scrapy http Response instance being processed
            spider --- The Scrapy spider instance
        '''
        # Get the proxy we used
        proxy_address = request.meta.get('proxy', None)
        if proxy_address and self.capping_active:
            # Decrement the proxy's CONNECTION_COUNT in the db
            self.update_conn_count(proxy_address, increase=False, debug=True)
        # Should return the response
        return response

    def process_exception(self, request, exception, spider):
        '''
        Errback

        Ensure the CONNECTION_COUNT for the proxy is decremented no matter what.

        Args:
            self --- The middleware class instance
            request --- The Scrapy http Request instance
            exception --- The exception that occurred
            spider --- The Scrapy spider instance
        Returns:
            None --- We always propagate along the process exception chain
                     (which means the next lower middleware process_exception
                      --- retry mw --- is called next)
        '''
        # If proxy always decrement no matter what
        proxy_address = request.meta.get('proxy', None)
        if proxy_address and self.capping_active:
            self.update_conn_count(proxy_address, increase=False, debug=True)
