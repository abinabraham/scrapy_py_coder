from scrapy.loader import ItemLoader
from scrapy.loader.processors import Compose, Join, Identity, MapCompose, TakeFirst
from w3lib.html import remove_tags
from scrapy_evo_common.loaders.loader_helper_funcs import get_dec, clean_uni

class CeWePhotoLoader(ItemLoader):

    # Used if fields don't specify one
    default_input_processor = Identity()

    cewe_master___product_name_in = Identity()
    cewe_master___short_description_in = Identity()
    cewe_master___base_url_in = Identity()
    cewe_master___image_urls_in = Identity()
    cewe_master___product_price_in =  Identity()
    cewe_master___product_price_out = Identity()
    cewe_master___description_in = MapCompose(unicode.title)
    cewe_master___product_category_in =  MapCompose(remove_tags, unicode.strip, clean_uni)
    cewe_master___product_category_out =  Join()
    cewe_master___product_bullet_descriptions_in = Identity()
    coques_master___merchant_product_id_in = MapCompose(remove_tags, unicode.strip, clean_uni, get_dec)

class CoquesPhotoLoader(ItemLoader):

    # Used if fields don't specify one
    default_input_processor = Identity()

    coques_master___product_name_in = Identity()
    coques_master___short_description_in = Identity()
    coques_master___base_url_in = Identity()
    coques_master___image_urls_in = Identity()
    coques_master___product_price_in = Identity()
    coques_master___product_price_out = Identity()
    coques_master___description_in = MapCompose(unicode.title)
    coques_master___product_category_out = Join()
    coques_master___product_category_in =  MapCompose(remove_tags, unicode.strip, clean_uni)
    coques_master___product_bullet_descriptions_in = Identity()
    coques_master___merchant_product_id_in = MapCompose(remove_tags, unicode.strip, clean_uni, get_dec)











class MypicLoader(ItemLoader):
    '''
    mypic_master

        product_name
        product_category
        product_format
        product_pages

    mypic_prices
        price_options
        extra_charges
        pocket_price
        full_price
        variant_name
        variant_pocket_price
        variant_full_price
    '''
    # Used if fields don't specify one
    default_input_processor = Identity()
    default_output_processor = TakeFirst()

    # Should be untouched
    mypic_prices___price_options_in = Identity()
    mypic_prices___price_options_out = Identity()

    # Prices
    mypic_prices___pocket_price_in = MapCompose(remove_tags, unicode.strip, clean_uni, get_dec)
    mypic_prices___full_price_in = MapCompose(remove_tags, unicode.strip, clean_uni, get_dec)
    mypic_prices___extra_charges_out = Join()
