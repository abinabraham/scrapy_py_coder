import base64
import logging
from scrapy.exceptions import NotConfigured
log = logging.getLogger(__name__)


class FixedProxyMeshIP(object):
    def __init__(self, settings):
        '''
        This middleware allows the use of proxy mesh in such a way that the
        proxy IP is fixed according to `PROXY_FORCED_IP` setting.

        It should be considered for testing use not production. To use it
        set the settings:
            FIXED_PROXY_ENDPOINT --- The proxymesh endpoint
            FIXED_PROXY_USERPASS --- The user:pass for proxymesh
            FIXED_PROXY_FORCED_IP --- The IP you wish to force (from those available on
                                that endpoint)
        N.B. You can get some IPs available with cURL:
            curl -I -x http://us.proxymesh.com:31280
            -U user:pass http://www.somesite.com |head -2|tail -1|cut -d ':' -f 2

        Then replace the random proxy middleware in `DOWNLOADER_MIDDLEWARES` with
        'scrapy_evo_common.downloadermiddlewares.fixedproxymeship.FixedProxyMeshIP': 100

        Just like the random proxy middleware this middleware uses the builtin Scrapy
        `HttpProxyMiddleware` so make sure that is also enabled in `DOWNLOADER_MIDDLEWARES`
        with a priority level such that it runs later. We set the meta key 'proxy'
        that `HttpProxyMiddleware` will utilize.

        Args:
            self --- The middleware class instance
            settings --- Crawler settings
        '''
        try:
            self.proxy_endpoint = settings.get('FIXED_PROXY_ENDPOINT')
            self.proxy_userpass = settings.get('FIXED_PROXY_USERPASS')
            self.proxy_forced_ip = settings.get('FIXED_PROXY_FORCED_IP')
        except KeyError:
            raise NotConfigured

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler.settings)

    def process_request(self, request, spider):
        '''
        Process the request and add the 'proxy' key to the meta and the auth headers
        needed, 'Proxy-Authorization'. Finally add the 'X-ProxyMesh-IP' to force
        Proxy Mesh to use this IP.

        Args:
            self --- The middleware class instance
            request --- The Scrapy http Request instance being processed
            spider --- The Scrapy spider instance
        '''
        if 'proxy' in request.meta:
            return
        request.meta['proxy'] = self.proxy_endpoint
        basic_auth = 'Basic ' + base64.b64encode(self.proxy_userpass).strip()  # mimic downloadermiddlewares.httpproxy
        request.headers['Proxy-Authorization'] = basic_auth
        # Force the IP
        request.headers['X-ProxyMesh-IP'] = self.proxy_forced_ip
        log.debug('Using proxy address: %s and forced IP: %s' % (self.proxy_endpoint, self.proxy_forced_ip))

    def process_exception(self, request, exception, spider):
        '''
        Errback.  Just log exception

        Args:
            self --- The middleware class instance
            request --- The Scrapy http Request instance
            exception --- The exception that occured
            spider --- The Scrapy spider instance
        '''
        log.info('FixedProxyMeshIP middleware exception was %s' % exception)
