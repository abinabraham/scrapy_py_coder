
import scrapy
# from scrapy_evo_common.utils.utils import make_valid_url

          


from scrapy.spiders import Spider
from scrapy.http import Request
from tutorial.items import FlexItem



class CeweSpider(Spider):

    name = "Cewe"
    allowed_domains = ['cewe.fr', ]
    start_urls = ['http://www.cewe.fr']

   
    def start_requests(self):
        '''
        Kick off. Make the extract_firstrst request to the target URL and callback to parse
        '''
        cewe_urls = self.start_urls

        for cewe_url in cewe_urls:
            self.logger.debug('Start requests %s' % cewe_url)
            yield scrapy.Request(url=cewe_url, callback=self.parse)


    def parse(self, response):
        '''
        Extract all category links

        cewe.fr can behave quite oddly. The splash page seems to need js, but
        if you just hit a category, it seems to work
        '''
        category_links = [('Partenaires Datas', 'http://www.cewe.fr/partenaires.html'),
                          ('Tirage Datas', 'http://www.cewe.fr/tirages-photo.html'),
                          ]
        for clink in category_links:
            cname = clink[0]
            chref = clink[1]
            # chref = make_valid_url(chref, reference_url=response.url)
            if cname == 'Partenaires Datas':
                self.logger.debug('Requesting %s category with URL %s' % (cname, chref))
                yield Request(url=chref, callback=self.parsePartenairesPhotos,
                              meta={'base_url': chref})
            if cname == 'Tirage Datas':
                self.logger.debug('Requesting %s category with URL %s' % (cname, chref))
                yield Request(url=chref, callback=self.parseTirageDatas,
                              meta={'base_url': chref})


    # ########################## Photos  CHAIN #############################

    def parsePartenairesPhotos(self, response):
        """

        Crawling the data from http://www.cewe.fr/tirages-photo.html
        Partenaires photos

        """
        cal_types = response.xpath('//div[@class="cw_nt"]')
        for cal_type in cal_types:
            partenaires_dict = {}
            partenaires_dict['partenaires__image_urls'] = cal_type.xpath('.//img/@src').extract_first()
            yield partenaires_dict

    def parseTirageDatas(self, response):
        """

        Crawling the data from http://www.cewe.fr/tirages-photo.html
        tirages photos and its title

        """
        cal_types = response.xpath('//div[@id="cw_teaser_wrapper"]')
        
        

        
        for cal_type in cal_types:
            tirage_dict = {}
            tirage_dict['tirage_to_banner_image'] = cal_type.xpath('//div[@class="cw_teaser_catpage_top_bigsize"]/img/@src').extract_first()
            tirage_dict['tirage_to_banner_image_title'] = cal_type.xpath('//div[@class="cw_teaser_catpage_top_bigsize"]/img/@src').extract_first()
            print 'cal_types+++++++++++++++++++',tirage_dict,cal_types
            yield tirage_dict


        #    