from bs4 import BeautifulSoup
import json


class BeautifulSoupMiddleware(object):
    def __init__(self, crawler):
        super(BeautifulSoupMiddleware, self).__init__()

        self.parser = crawler.settings.get('BEAUTIFULSOUP_PARSER', "html.parser")

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def process_response(self, request, response, spider):
        """Overridden process_response would "pipe" response.body through BeautifulSoup."""
        # Remember that images won't be redownloaded until age > IMAGES_EXPIRE
        if hasattr(request, 'meta'):
            images_pipeline = request.meta.get('IMAGES_PIPELINE', False)
            if images_pipeline:
                # Do nothing for images
                return response

        # Another check for images (e.g. when we render png with splash for
        # screenshots)
        if response.headers.get('Content-Type') == 'image/png':
            return response

        # Determine if response is JSON or HTML
        try:
            json.loads(response.body)
            # For JSON responses, do not parse with bs4
            return response
        except ValueError:
            # Not JSON response, parse
            cleaned_body = BeautifulSoup(response.body, self.parser, from_encoding=response.encoding)
            return response.replace(body=cleaned_body.encode(response.encoding))
