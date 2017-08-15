from __future__ import division
from scrapy.downloadermiddlewares.stats import DownloaderStats
from scrapy.utils.response import response_httprepr


class CustomDownloaderStats(DownloaderStats):
    '''
    Customize Scrapy's DownloaderStats middleware.
    We keep track of total response latency for images and non-images,  how
    many responses were image or non-image, total kb for image and non-image

    This can be occassionally useful when trying to debug bottleneck or refine
    optimum settings.

    To enable this middleware, in `DOWNLOADER_MIDDLEWARES` setting:
        'scrapy.downloadermiddlewares.stats.DownloaderStats': None,  # Disable the built-in
        'scrapy_evo_common.downloadermiddlewares.stats.CustomDownloaderStats': 850
    '''
    def process_response(self, request, response, spider):
        # spider.log('Latency: %.2f' % request.meta.get('download_latency'))
        image_request = request.meta.get('IMAGES_PIPELINE', False)
        reslen = len(response_httprepr(response))/1024 # Convert to kb
        if image_request:
            self.stats.inc_value('downloader/images/total_latency', request.meta.get('download_latency'), spider=spider)
            self.stats.inc_value('downloader/images/response_count', 1, spider=spider)
            self.stats.inc_value('downloader/images/response_kilobytes',
                                 float("{0:.2f}".format(reslen)), spider=spider)
        else:
            self.stats.inc_value('downloader/non_images/total_latency', request.meta.get('download_latency'), spider=spider)
            self.stats.inc_value('downloader/non_images/response_count', 1, spider=spider)
            self.stats.inc_value('downloader/non_images/response_kilobytes',
                                 float("{0:.2f}".format(reslen)), spider=spider)
        super(CustomDownloaderStats, self).process_response(request, response, spider)
        return response
