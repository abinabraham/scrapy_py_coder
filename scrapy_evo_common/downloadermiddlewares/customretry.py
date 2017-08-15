from scrapy.downloadermiddlewares.retry import RetryMiddleware
from twisted.internet import defer
from twisted.internet.error import TimeoutError, DNSLookupError, \
    ConnectionRefusedError, ConnectionDone, ConnectError, \
    ConnectionLost, TCPTimedOutError
from scrapy.xlib.tx import ResponseFailed
from scrapy.utils.response import response_status_message
from .randomproxy import ProxyPoolExhausted


class CustomRetryMiddleware(RetryMiddleware):
    # Catch ProxyPoolExhausted exception too, so we retry in this case
    # NB this catches exceptions raised in process_request not process_response
    EXCEPTIONS_TO_RETRY = (defer.TimeoutError, TimeoutError, DNSLookupError,
                           ConnectionRefusedError, ConnectionDone, ConnectError,
                           ConnectionLost, TCPTimedOutError, ResponseFailed,
                           IOError, ProxyPoolExhausted)

    def process_response(self, request, response, spider):
        ''' Also retry empty html responses '''
        if request.meta.get('dont_retry', False):
            return response
        if response.status in self.retry_http_codes:
            reason = response_status_message(response.status)
            return self._retry(request, reason, spider) or response
        elif hasattr(response, 'body'):
            if (response.body == '<html><head></head><body></body></html>' or response.body == '<html></html>'
               or response.body == '<html><body></body></html>'):
                # Blank responses also trigger a retry
                reason = 'The response HTML was empty'
                return self._retry(request, reason, spider) or response
        return response
