from scrapy.spiders import Spider
from scrapy.loader.processors import TakeFirst
from scrapy.http import Request
from scrapy.loader.processors import Compose
from scrapy.exceptions import CloseSpider
from scrapy_evo_common.utils.utils import make_valid_url, _build_loader_class
from scrapy_evo_common.loaders.loader_helper_funcs import clean_uni, get_dec
from w3lib.html import remove_tags
import hashlib
from tutorial.loaders import CeWePhotoLoader, CoquesPhotoLoader
from tutorial.items import FlexItem
import urlparse

take_first = TakeFirst()


def clean_price(price_string):
    m = Compose(remove_tags, unicode.strip, clean_uni, get_dec)
    return m(price_string)


class CeweSpider(Spider):

    name = "cewe"
    allowed_domains = ["cewe.fr", ]

    start_urls = ['https://cewe.fr/']




    def start_requests(self):
        '''
        Kick off. Make the first request to the target URL and callback to parse
        '''
        # self.logger.debug('Start requests %s' % self.merchant_target_url)
        yield Request(url=self.start_urls[0], callback=self.parse)



    def parse(self, response):
        '''
        Extract all category links

        cewe.fr can behave quite oddly. The splash page seems to need js, but
        if you just hit a category, it seems to work
        '''
        category_links = [('Tirage Datas', 'http://www.cewe.fr/tirages-photo.html'),
                          ('Coques Personnalisees', 'http://www.cewe.fr/coques-personnalisees.html'),
                          ]
        for clink in category_links:
            cname = clink[0]
            chref = clink[1]
            chref = make_valid_url(chref, reference_url=response.url)
            if cname == 'Tirage Datas':
                self.logger.debug('Requesting %s category with URL %s' % (cname, chref))
                yield Request(url=chref, callback=self.parseTirageListing,
                              meta={'base_url': chref})
            if cname == 'Coques Personnalisees':
                self.logger.debug('Requesting %s category with URL %s' % (cname, chref))
                yield Request(url=chref, callback=self.parseCoquesListing,
                              meta={'base_url': chref})

    ########################### Tirage  List #############################
    def parseTirageListing(self, response):
        cal_types = response.xpath('//div[@id="cw_teaser_wrapper"]//div[@class="cw_teaser   cw_newteaser cw_teaser_background  "]')
        for cal_type in cal_types:
            product_dict = {}
            product_dict['cewe_master___base_url'] = response.meta.get('base_url')
            product_dict['cewe_master___source_link'] = source_link = cal_type.xpath('.//a/@href').extract_first()
            # if cal_type.xpath('.//a/h2[@class="cw_teaser_hl_text"]/font/font/text()'):
            if cal_type.css('h2.cw_teaser_hl_text::text'):
                product_dict['cewe_master___product_name'] = clean_uni(cal_type.css('h2.cw_teaser_hl_text::text').extract_first().strip())
            else:
                product_dict['cewe_master___product_name'] = None
            product_dict['cewe_master___short_description'] = clean_uni(cal_type.xpath('.//div[contains(@class, "cw_teaser_text")]/text()[string-length(normalize-space(.))>0]').extract_first())
            yield Request(make_valid_url(source_link), callback=self.parseTirageCat,
                          meta={'product_dict': product_dict})

    ########################### Tirage  Category #############################
    def parseTirageCat(self, response):
        product_dict = response.meta.get('product_dict')
        product_dict['cewe_master___product_category'] = clean_uni(response.xpath('//h1[@class="cw_teaser_hl cw_teaser_hl_auto cw_teaser_hl_border"]/text()').extract_first())
        product_dict['cewe_master___product_category_description_short'] = response.xpath('//h2[@class="cw_teaser_hl"]/text()').extract_first()
        if product_dict.get('cewe_master___product_name') and product_dict.get('cewe_master___product_category'):
            hash_input = '%s_%s' % (clean_uni(product_dict['cewe_master___product_category']),
                                    clean_uni(product_dict['cewe_master___product_name']))
            product_dict['cewe_master___merchant_product_id'] = '%s' % hashlib.md5(hash_input).hexdigest()


        price_option = clean_price(response.xpath('//p[@id="pip_price"]/text()').extract_first())
        img_urls = []
        img_urls = [urlparse.urljoin(response.url, src) \
                    for src in response.xpath('//div[@class="detail_image"]//img/@data-original').extract()]


        print "img_urls",img_urls
        product_dict['cewe_master___image_urls']= img_urls
        product_dict['cewe_master___product_price'] = price_option
        bullet_description_ul = response.xpath('//ul[@id="pip_bullet_points"]')
        bullet_li = []
        for li in bullet_description_ul:
                bullet_li_txt = li.xpath('//li[@class="pip_bullet_point"]/text()'.encode('utf8')).extract()
                bullet_li.append(bullet_li_txt)
        product_dict['cewe_master___product_bullet_descriptions'] = bullet_li
        product_dict['cewe_master___description'] = response.xpath('//div[@class="pip_tab_content"]//div[@class="detail_text"]/p[@class="detail_text"]/text()[string-length(normalize-space(.))>0]').extract()[:2]

        ldr = CeWePhotoLoader(item=FlexItem(), response=response)
        ldr.context['spider_currency'] = 'EUR'  # Set the currency for the loader
        ldr.context['reference_url'] = response.url
        for key, val in product_dict.iteritems():
            ldr.add_value(key, val)
        yield ldr.load_item()


    ########################### Coques  List #############################
    def parseCoquesListing(self, response):
        cal_types = response.xpath('//div[@id="cw_content"]//div[@class="cw_teaser  cw_teaser_border cw_newteaser cewe-newteaser-half-page   "]')
        for cal_type in cal_types:
            product_dict = {}
            product_dict['coques_master___base_url'] = response.meta.get('base_url')
            product_dict['coques_master___source_link'] = source_link = cal_type.xpath('.//a/@href').extract_first()
 
            if cal_type.css('h2.cw_teaser_hl_text::text'):
                product_dict['coques_master___product_name'] = clean_uni(cal_type.css('h2.cw_teaser_hl_text::text').extract_first().strip())
            else:
                product_dict['coques_master___product_name'] = None
            product_dict['coques_master___image_urls'] = cal_type.xpath('.//img/@src').extract()
            product_dict['coques_master___short_description'] = clean_uni(cal_type.xpath('.//div[contains(@class, "cw_teaser_text")]/text()[string-length(normalize-space(.))>0]').extract_first())
            yield Request(make_valid_url(source_link), callback=self.parseCoquesCat,
                          meta={'product_dict': product_dict})

        ########################### coques  Category #############################
    def parseCoquesCat(self, response):
        product_dict = response.meta.get('product_dict')
        product_dict['coques_master___product_category'] = clean_uni(response.xpath('//h1[@class="cw_teaser_hl cw_teaser_hl_auto cw_teaser_hl_border"]/text()').extract_first())
        product_dict['coques_master___product_category_description_short'] = response.xpath('//h2[@class="cw_teaser_hl"]/text()').extract_first()
        if product_dict.get('coques_master___product_name') and product_dict.get('cewe_master___product_category'):
            hash_input = '%s_%s' % (clean_uni(product_dict['ccoques_master___product_category']),
                                    clean_uni(product_dict['coques_master___product_name']))
            product_dict['coques_master___merchant_product_id'] = '%s' % hashlib.md5(hash_input).hexdigest()
        

        price_option = clean_price(response.xpath('//p[@id="pip_price"]/text()').extract_first())         
        product_dict['coques_master___product_price'] = price_option
        bullet_description_ul = response.xpath('//ul[@id="pip_bullet_points"]')
        bullet_li = []
        for li in bullet_description_ul:
            bullet_li_txt = li.xpath('//li[@class="pip_bullet_point"]/text()').extract()

            bullet_li.append(bullet_li_txt)
        product_dict['coques_master___product_bullet_descriptions'] = bullet_li
        product_dict['coques_master___description'] = response.xpath('//div[@class="pip_tab_content"]//div[@class="detail_text"]/p[@class="detail_text"]/text()[string-length(normalize-space(.))>0]').extract()[:2]


        ldr = CoquesPhotoLoader(item=FlexItem(), response=response)
        ldr.context['spider_currency'] = 'EUR'  # Set the currency for the loader
        ldr.context['reference_url'] = response.url
        for key, val in product_dict.iteritems():
            ldr.add_value(key, val)
        yield ldr.load_item()
