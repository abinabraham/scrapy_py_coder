# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
from scrapy.loader.processors import TakeFirst
take_first = TakeFirst()


def item2str(item):
    '''
    Helper function to take an item and
    format it as a pretty str
    '''
    item_strs = []
    for (k, v) in item.iteritems():
        # Ensure the key is unicode (floats/ints cast to unicode)
        if (type(v) == list):
            continue
        temp_k = k
        if(type(k) == int or type(k) == float):
            temp_k = unicode(k)
        temp_v = v
        # Ensure the val is unicode (floats/ints cast to unicode)
        if(type(v) == int or type(v) == float):
            temp_v = unicode(v)
        # Sometimes plain str, like more images url
        if(type(temp_k) == str):
            try:
                temp_k = temp_k.decode('utf8')
            except Exception:
                pass
        if(type(temp_v) == str):
            try:
                temp_v = temp_v.decode('utf8')
            except Exception:
                pass
        # Should now be unicode
        if(type(temp_k) == unicode and type(temp_v) == unicode):
            item_strs.append(u'%s = > %s' % (temp_k, temp_v))
        elif(type(temp_k) == unicode and type(temp_v) == dict):
            # price options dict
            pass
        else:
            raise TypeError('Could not format item to string with key type %s'
                            ' and value type %s' % (type(temp_v), type(temp_k)))
    return u'\n'.join(item_strs)


class PhotoProductPipeline(object):
    '''
     Primary function is to drop duplicates. If merchant_product_id number already seen
     we consider it to be a duplicate.

     We also drop items without the required fields price or productName

     If full_price is more than pocket_price that also indicates an error
    '''

    def __init__(self):
        self.products_seen = set()

    def process_item(self, item, spider):
        '''
        Called for each item

        Args:
            self --- Instance of pipeline
            item --- The item being processed
            spider -- Instance of the spider
        Returns:
            item
        '''
        # We dont deal with screenshots
        if item.get('item_type') == 'screenshot':
            return item

        # Check we have all the required fields
        if hasattr(spider, 'required_base_fields'):
            required_fields = spider.required_base_fields
        else:
            required_fields = ['product_name', 'merchant_product_id',
                               'base_url', 'source_link', 'image_urls']
        missing_fields = []
        for rf in required_fields:
            # Try to get at least one non-null field in item that endswith
            # required field name
            if len([fld for fld in item.keys() if fld.endswith('___' + rf)
                    and item.get(fld, None) is not None]) == 0:
                missing_fields.append(rf)
        missing_fields = u', '.join(missing_fields)
        if missing_fields:
            item_str = item2str(item)
            raise DropItem(u'Item missing %s required field(s) so dropped'
                           u' src URL: %s\n Item: \n%s' %
                           (missing_fields, item.get('source_link', None), item_str))

        # Check item has at least one of these price fields (if ends in
        # case_pocket_price, also ends in just pocket_price, we check if
        # 'pocket_price' in the field, because variant fields may end with
        # something like 'pocket_price___bottle' etc
        # It's debatable whether we should keep this drop check or just allow
        # the sql check to pick it up and then view explicitly the item and the
        # missing fields.
        # Check we have all the required fields
        if hasattr(spider, 'required_price_fields'):
            required_price_fields = spider.required_price_fields
        else:
            required_price_fields = ['pocket_price', ]
        missing_price_fields = []
        for rpf in required_price_fields:
            if len([pfld for pfld in item.keys() if rpf in pfld
                    and item.get(pfld) is not None]) == 0:
                missing_price_fields.append(rpf)

        if len(missing_price_fields) != 0:
            raise DropItem(u'Item has none of the types of pocket price field so dropped'
                           u' src URL: %s\n Item: \n%s' %
                           (item.get('source_link'), item2str(item)))

        # Assume product merchant_product_id unique. Drop duplicates.
        merchant_product_id_field_name = take_first([fnm for fnm in item.keys()
                                                     if fnm.endswith('___merchant_product_id')])
        if merchant_product_id_field_name:
            merchant_product_id = item.get(merchant_product_id_field_name, None)
            if merchant_product_id:
                # Drop duplicates
                if merchant_product_id in self.products_seen:
                    raise DropItem(u'Duplicate item found with merchant product id: %s'
                                   % merchant_product_id)
                else:
                    # Add tuple to already seen set
                    self.products_seen.add(merchant_product_id)
            else:
                dropmsg = u'Item has no merchant_product_id.'
                dropmsg += u'\nThe item looks like:\n%s\n' % item2str(item)
                raise DropItem(dropmsg)
        else:
            dropmsg = u'Item has no merchant_product_id.'
            dropmsg += u'\nThe item looks like:\n%s\n' % item2str(item)
            raise DropItem(dropmsg)

        return item
