# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import os
import pytz
import six
from scrapy.exceptions import DropItem
from scrapy.loader.processors import TakeFirst
from scrapy.pipelines.images import ImagesPipeline
from scrapy.pipelines.files import FSFilesStore, S3FilesStore
from scrapy.http import Request
from scrapy.utils.datatypes import CaselessDict
from collections import defaultdict
from twisted.internet import threads
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
        else:
            raise TypeError('Could not format item to string with key type %s'
                            ' and value type %s' % (type(temp_v), type(temp_k)))
    return u'\n'.join(item_strs)


class AzureFilesStore(object):

    AZURE_ACCOUNT_NAME = None
    AZURE_SECRET_ACCESS_KEY = None

    HEADERS = {
        'Cache-Control': 'max-age=172800',
    }

    def __init__(self, uri):
        from azure.storage.blob import BlockBlobService, ContentSettings
        self.BlockBlobService = BlockBlobService
        self.ContentSettings = ContentSettings
        assert uri.startswith('azure://')
        self.container, self.prefix = uri[8:].split('/', 1)

    def stat_file(self, path, info):
        def _onsuccess(blob_properties):
            if blob_properties:
                checksum = blob_properties.properties.etag.strip('"')
                # Aware datetime
                last_modified = blob_properties.properties.last_modified
                # N.B. strftime("%s") currently is broken and doesn't respect tzinfo
                utc_last_modified = last_modified.astimezone(pytz.utc)
                modified_stamp = int(utc_last_modified.strftime("%s"))
                return {'checksum': checksum, 'last_modified': modified_stamp}
            # If media_to_download gets a None result it will also return None
            # and force download
            return None

        return self._get_azure_blob(path).addCallback(_onsuccess)

    def _get_azure_service(self):
        # If need http instead of https, use protocol kwarg
        return self.BlockBlobService(account_name=self.AZURE_ACCOUNT_NAME, account_key=self.AZURE_SECRET_ACCESS_KEY)

    def _get_azure_blob(self, path):
        blob_name = '%s%s' % (self.prefix, path)
        # Check if blob exists
        s = self._get_azure_service()
        # Note: returning None as result will force download in media_to_download
        if s.exists(self.container, blob_name=blob_name):
            # Get properties
            return threads.deferToThread(s.get_blob_properties, self.container, blob_name=blob_name)
        return threads.deferToThread(lambda _: _, None)

    def persist_file(self, path, buf, info, meta=None, headers=None):
        """Upload file to Azure blob storage"""
        blob_name = '%s%s' % (self.prefix, path)
        extra = self._headers_to_azure_content_kwargs(self.HEADERS)
        if headers:
            extra.update(self._headers_to_azure_content_kwargs(headers))
        buf.seek(0)
        s = self._get_azure_service()
        return threads.deferToThread(s.create_blob_from_bytes, self.container, blob_name, buf.getvalue(),
                                     metadata={k: str(v) for k, v in six.iteritems(meta or {})},
                                     content_settings=self.ContentSettings(**extra))

    def _headers_to_azure_content_kwargs(self, headers):
        """ Convert headers to Azure content settings keyword agruments.
        """
        # This is required while we need to support both boto and botocore.
        mapping = CaselessDict({
            'Content-Type': 'content_type',
            'Cache-Control': 'cache_control',
            'Content-Disposition': 'content_disposition',
            'Content-Encoding': 'content_encoding',
            'Content-Language': 'content_language',
            'Content-MD5': 'content_md5',
            })
        extra = {}
        for key, value in six.iteritems(headers):
            try:
                kwarg = mapping[key]
            except KeyError:
                raise TypeError(
                    'Header "%s" is not supported by Azure' % key)
            else:
                extra[kwarg] = value
        return extra


class CustomImagesPipeline(ImagesPipeline):
    '''
    Custom images pipeline modifying Scrapy's by providing storage on Azure
    (instead of just s3), and some other small changes:

    --- We add the `IMAGES_PIPELINE` tag to the meta for easy identification
        of image requests in other middlewares
    --- Instead of relative path we build an absolute path (either absolute filesys
        path or absolute URL for S3 or Azure) and store it in the `image_path` field of the item

    If we persist files to say s3/azure, the proxy downloader middleware should
    not apply, since are not firing Scrapy http Requests. This is important
    for not wasting proxy bandwidth (I verified with wireshark that the direct
    connection to azure IP is being made)

    N.B.
    `IMAGES_URLS_FIELD` is by default `image_urls`
    `IMAGES_RESULT_FIELD` is by default `images`
    '''

    STORE_SCHEMES = {
        '': FSFilesStore,
        'file': FSFilesStore,
        's3': S3FilesStore,
        'azure': AzureFilesStore,
    }

    @classmethod
    def from_settings(cls, settings):
        azureStore = cls.STORE_SCHEMES['azure']
        azureStore.AZURE_ACCOUNT_NAME = settings['AZURE_ACCOUNT_NAME']
        azureStore.AZURE_SECRET_ACCESS_KEY = settings['AZURE_SECRET_ACCESS_KEY']
        s3store = cls.STORE_SCHEMES['s3']
        s3store.AWS_ACCESS_KEY_ID = settings['AWS_ACCESS_KEY_ID']
        s3store.AWS_SECRET_ACCESS_KEY = settings['AWS_SECRET_ACCESS_KEY']

        project_root = settings['PROJECT_ROOT']
        store_uri = settings['IMAGES_STORE']
        return cls(store_uri, project_root=project_root, settings=settings)

    def __init__(self, *args, **kwargs):
        self.project_root = kwargs.pop('project_root', None)
        super(CustomImagesPipeline, self).__init__(*args, **kwargs)

    def get_media_requests(self, item, info):
        '''
        Add the `IMAGES_PIPELINE` meta to more easily identify image requests
        in other middlewares etc

        Args:
            self --- The pipeline instance
            item --- The item currently being processed
            info --- spiderinfo (spider, downloading, downloaded, waiting)
        '''
        # Since in general the field name will be prefixed by our master table
        # name(e.g. 'aldi_master___image_urls', when IMAGES_URLS_FIELD =
        # 'image_urls'), we want to find the field name taking that into account
        full_images_urls_field = take_first([field_name for field_name in item.fields
                                             if field_name.endswith(self.images_urls_field)])
        if full_images_urls_field:
            return [Request(x, meta={'IMAGES_PIPELINE': True})
                    for x in item.get(full_images_urls_field, [])]
        return []

    def item_completed(self, results, item, info):
        '''
        Make the path absolute rather than relative. Do this accordingly
        for the case of FS storage, S3 storage or Azure storage. Add this absolute path
        to the `images_results_field` field of the item (set by user in settings.py)

        Args:
            self --- The pipeline instance
            results --- The results we downloaded
            item --- The item currently being processed
            info --- spiderinfo (spider, downloading, downloaded, waiting)
        Returns:
            The item
        '''
        # Our FlexItem has defaultdict fields so we admit that possibility too
        if isinstance(item, dict) or isinstance(item.fields, defaultdict) or self.images_result_field in item.fields:
            paths = [x['path'] for ok, x in results if ok]
            abs_paths = []
            for path in paths:
                if type(self.store) == FSFilesStore:
                    abs_paths.append(os.path.join(self.project_root, 'images', path))
                elif type(self.store) == S3FilesStore:
                    if self.store.prefix:
                        abs_paths.append('http://%s/%s/%s/%s'
                                         % (self.store.S3Connection.DefaultHost,
                                            self.store.bucket, self.store.prefix, path))
                    else:
                        abs_paths.append('http://%s/%s/%s' % (self.store.S3Connection.DefaultHost,
                                                              self.store.bucket, path))
                elif type(self.store) == AzureFilesStore:
                    if self.store.prefix:
                        abs_paths.append('http://%s/%s/%s/%s' %
                                         (self.store._get_azure_service().primary_endpoint,
                                          self.store.container, self.store.prefix, path))
                    else:
                        abs_paths.append('http://%s/%s/%s' %
                                         (self.store._get_azure_service().primary_endpoint,
                                          self.store.container, path))
            try:
                master_table_name = info.spider.master_table.table_name
                # Store on the master table if it's configured
                item['___'.join([master_table_name, self.images_result_field])] = abs_paths
            except Exception:
                item[self.images_result_field] = abs_paths
        return item

    # def get_images(self, response, request, info):
    #     '''
    #     As far as I know Pillow can't deal with JPEG-XR (JXR) files
    #     , but with libjxr-dev installed ImageMagick can convert to friendlier format,
    #     so if we have a JXR file a hacky soln is to write it to a temp file on disk
    #     , run convert on it, then read the converted file back in
    #     **Might be hard to do this without blocking...almost def an in-memory soln
    #     is needed...** Prob best not to use this method..!!
    #     '''
    #     try:
    #         # Pre-check test if Pillow can process this type of image
    #         from PIL import Image as PImage
    #         from cStringIO import StringIO as BytesIO
    #         PImage.open(BytesIO(response.body))
    #         # log.debug('get_images: no problem opening with PIL....')
    #     except IOError as e:
    #         if 'cannot identify image file' in e.message:
    #             log.error('PIL could not id, attempt some crazyness. url:%s, response headers:%s' % (response.url, response.headers))
    #             # Maybe it's a jxr img, temp save, and try imagemagick covert
    #             # read back in etc
    #             try:
    #                 # Clunky method attempting to take a jxr image --> tiff
    #                 # image with horrible blocking writes to disk in the middle
    #                 import tempfile
    #                 import subprocess
    #                 import os
    #                 fileTemp = tempfile.NamedTemporaryFile(delete=False, suffix='.jxr')
    #                 outFileName = os.path.splitext(fileTemp.name)[0]+'.tif'
    #                 fileTemp.write(response.body)
    #                 fileTemp.close()
    #                 subprocess.check_call(["convert", fileTemp.name, outFileName])
    #                 os.remove(fileTemp.name)
    #                 with open(outFileName, 'rb') as tiffile:
    #                     response = response.replace(body=tiffile.read())
    #                 os.remove(outFileName)
    #             except Exception as e:
    #                 log.error('PIL could not identify image and I failed to convert'
    #                           ' to tif from jxr. Exception was:\n%s' % e)
    #         else:
    #             # log.error('A different IOError msg: %s' % e.message)
    #             pass
    #     except Exception as e:
    #         # Not interested in dealing with other issues here
    #         # log.error('Another random error: %s' % e)
    #         pass

    #     # Let Scrapy ImagesPipeline get_images continue
    #     return super(CustomImagesPipeline, self).get_images(response, request, info)


class DefaultValuesPipeline(object):
    '''
    Pipeline that sets default values for items scraped
    '''
    def process_item(self, item, spider):
        '''
        Set default

        Args:
            self --- The instance of the pipeline
            item --- The item being processed
            spider --- The spider instance
        Returns:
            The item
        '''
        if spider.name == 'Booking':
            # It seems not all hotels on booking.com have ratings..
            # and without the default Scrapy will just omit the rooms_list
            # from item keys if rooms_list is empty list or None.
            item.setdefault('rooms_list', [])
        elif spider.name == 'Airbnb':
            pass
        return item


class TrainsPipeline(object):
    '''
    Drop any items without required fields.

    Drop duplicate journeys (journeys are distinguished by departure/arrival
    time and station.
    '''
    def __init__(self):
        self.journeys_seen = set()

    def process_item(self, item, spider):
        '''
        Drop any items without required fields or already seen

        Args:
            self --- Instance of pipeline
            item --- The item being processed
            spider -- Instance of the spider
        Returns:
            item
        '''
        if spider.name == 'Trenitalia' or spider.name == 'Italotreno':
            # If any of these are None, trying to insert into db without will
            # result in an error, so item should be dropped
            required_fields = ['departurestation', 'departuretime', 'arrivalstation', 'arrivaltime', 'departuredate']
            missing_fields = u', '.join([rf for rf in required_fields if item.get(rf) is None])
            if missing_fields:
                raise DropItem(u'Item missing %s required field(s) so dropped '
                               u'. Item: \n%s' %
                               (missing_fields, item2str(item)))

            # Guard against duplicate journeys
            journey_tuple = (item.get('departurestation'), item.get('departuretime'),
                             item.get('arrivalstation'), item.get('arrivaltime'), item.get('departuredate'))
            if journey_tuple in self.journeys_seen:
                raise DropItem('Duplicate journey found:'
                               ' (%s, %s, %s, %s, %s)'
                               % (item.get('departurestation'), item.get('departuretime'),
                                  item.get('arrivalstation'), item.get('arrivaltime'), item.get('departuredate')))
            else:
                # Add tuple to already seen set
                self.journeys_seen.add(journey_tuple)

        return item


class HotelsPipeline(object):
    '''
    Drop any items without required fields.
    Drop any items with merchant_hotel_id and date_booking
    that we've already seen.

    N.B. We don't drop duplicates with merchant_hotel_id
    since we are iterating over multiple checkin dates and expect
    to see the same hotel several times.
    '''
    def __init__(self):
        self.id_dates_seen = set()

    def process_item(self, item, spider):
        '''
        Drop any items without required fields or already seen

        Args:
            self --- Instance of pipeline
            item --- The item being processed
            spider -- Instance of the spider
        Returns:
            item
        '''
        if spider.name == 'Airbnb':
            # If any of these are None, trying to insert into db without will
            # result in an error, so item should be dropped
            required_fields = ['HostName', 'UserID', 'source_link', 'date_booking',
                               'merchant_hotel_id', 'baseURL', 'hotel_name', 'hotel_description',
                               'hotel_coord_lat', 'hotel_coord_lon']
            missing_fields = u', '.join([rf for rf in required_fields if item.get(rf, None) is None])
            if missing_fields:
                raise DropItem(u'Item missing %s required field(s) so dropped (all unicode field entry?)'
                               u'src URL: %s\n Item: \n%s' %
                               (missing_fields, item.get('source_link', None), item2str(item)))

            # If Airbnb we must guard against duplicate pairs
            # (merchant_hotel_id, date_booking) as sometimes rental appears on
            # multiple pages of result for given checkin date.
            id_date_tuple = (item.get('merchant_hotel_id'), item.get('date_booking'))
            if id_date_tuple in self.id_dates_seen:
                raise DropItem(u'Duplicate merchant_hotel_id-dating_booking item found:'
                               u' (%s, %s) from URL: %s'
                               % (item.get('merchant_hotel_id'), item.get('date_booking'),
                                  item.get('baseURL')))
            else:
                # Add tuple to already seen set
                self.id_dates_seen.add(id_date_tuple)
        elif spider.name == 'Booking':
            # If any of these are None, trying to insert into db without will
            # result in an error, so item should be dropped
            # N.B. Room level dropping is taken care in the loader (and required
            # fields like `date_booking` are checked there)
            required_fields = ['hotel_date_listed', 'source_link', 'hotel_name',
                               'merchant_hotel_id', 'hotel_description', 'hotel_coord_lat',
                               'hotel_coord_lon', 'rooms_list']
            missing_fields = u', '.join([rf for rf in required_fields if item.get(rf, None) is None])
            if missing_fields:
                raise DropItem(u'Item missing %s required field(s) so dropped (all unicode field entry?)'
                               u'src URL: %s\n Item: \n%s' %
                               (missing_fields, item.get('source_link', None), item2str(item)))

        return item


class ProductPipeline(object):
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

        # If current price is greater than orig price, that is weird.
        # full_price key
        pocket_price_field_name = take_first([fname for fname in item.keys() if fname.endswith('___pocket_price')])
        full_price_field_name = take_first([fname for fname in item.keys() if fname.endswith('___full_price')])
        if (pocket_price_field_name is not None and full_price_field_name is not None and
           item[pocket_price_field_name] is not None and item[full_price_field_name] is not None):
            try:
                fprice = float(item[full_price_field_name])
                pprice = float(item[pocket_price_field_name])
                if pprice > fprice:
                    raise DropItem(u'Pocket price %s greater than full price %s. Drop.'
                                   % (pprice, fprice))
            except ValueError:
                # one of the prices might be N/A
                pass

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
