# The MSSQL Writer pipeline (not stored in pipelines.py because of the size)
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
import datetime
from scrapy.loader.processors import TakeFirst
from scrapy_evo_common.pipelines.mssqlwriter import BaseMSSQLWriter
from collections import defaultdict
# from scrapy.conf import settings
take_first = TakeFirst()


class MSSQLWriter(BaseMSSQLWriter):

    def _format_item(self, item, spider):
        '''
        Takes our flat Scrapy Item, which has fields named according to the following
        convention: <table_name>___<column_name>, and reformats the item into
        a more manageably nested JSON structure, with tables > columns

        This item is a defaultdict of defaultdicts. The second defaultdict being such
        that is returns None when queried for a key that doesn't exist. E.g.
        fitem['x'] => defaultdict
        fitem['x']['y'] = > None     (assuming non-existant keys)
        This is useful because often we will have a column in a table of our schema
        that we haven't scraped data for in this item, yet we still want to generate
        a null entry in the data tuple we insert to match up with the placeholder etc.

        The column `price_options' is special in that it stores a list
        of variant pricing option dictionaries, each with a 'name' key and some other
        keys. We don't call it 'variant_options' because the loader builder would not
        build the variant loader correctly for variant fields and we need identity.
        A seperate price row should be generated for each of these with the name
        column taking it's value accordingly, e.g. [{'variant_name': 'Size: 12x12', 'variant_max_price': 6,
        'variant_min_price': 4}, ...], this list should not be joined. This is different to how we deal
        with variants in the retail spider.
        '''
        fitem = defaultdict(lambda: defaultdict(lambda: None))
        for k, v in item.iteritems():
            x = k.split('___')  # '___' delimits table name, column name
            if isinstance(v, list) and not k.endswith('price_options'):
                # Usually loaders would deal with this, but we need to load image_urls
                # as list of urls in order for image pipeline to work.
                v = ', '.join(v)
            if len(x) == 2:
                tbl, col = x
                fitem[tbl][col] = v
            else:
                spider.logger.error('The key %s is invalid. Format should be `<table_name>___<column_name>`'
                                    % k)
        return fitem

    def _do_upsert(self, curs, item, spider):
        """
        SQL upsert for the retail spider

        For products we update if exist already and insert if not.
        With a guaranteed product row for this merchant_product_id now in the db.
        We continue to insert the prices.  We continue to append a price row for the product.

        N.B.  We may have existing prices for this product, but we still
        append the price again with run_id to today's run id, because
        we may want compare how price changes over dates.

        Args:
            self --- The MSSQLWriter instance
            curs --- The database cursor
            item --- The Scrapy item that we wish to write to database
            spider --- The Scrapy spider instance

        """
        date_now = datetime.datetime.utcnow().strftime('%Y-%m-%d')

        if item.get('item_type') == 'screenshot':
            # This pipeline does not deal with screenshots
            return item

        # Get formatted item
        fitem = self._format_item(item, spider)

        # First add product to the master table
        dbt = spider.master_table
        ordered_columns = [dbc for dbc in dbt.dbcolumns.all().order_by('column_name')
                           if dbc.column_type != 'TIMESTAMP' and not dbc.primary_id_column]
        # Add data for extra columns that aren't in scraped data (we should have an
        # entry under the table for every one of its columns except timestamp/primary cols.
        # This entry may be null, and that's fine. fitem generates defaultdict if table key
        # doesn't exist, None if column doesn't exist)
        fitem[dbt.table_name].update({'run_id': self.run_id, 'merchant_id': self.merchant_id,
                                      'date_start': date_now})

        merchant_product_id = fitem[dbt.table_name]['merchant_product_id']
        if merchant_product_id is None:
            spider.logger.error('Got None for merchant product id is table %s' % dbt.table_name)
            return
        merchant_product_id = str(merchant_product_id)
        spider.logger.debug('Got merchant_product_id:%s' % merchant_product_id)
        # Is there a product entry for this merchant_product_id?
        products_exists_cmd = "SELECT 1 FROM %s WHERE merchant_product_id = ?;" % dbt.table_name
        curs.execute(products_exists_cmd, (merchant_product_id, ))
        products_ret = curs.fetchone()
        if products_ret:
            products_ret = products_ret[0]

        # Data for the upsert (we don't touch the timestamp/primary cols)
        data_tuple = tuple(fitem[dbt.table_name][dbc.column_name] for dbc in ordered_columns)

        if products_ret:
            # There is already a product entry for this merchant_product_id
            # Set `date_start` to current date as this is an update for existing row.
            # REMEMBER: NO COMMA BEFORE 'WHERE'
            spider.logger.info('Attempt to update %s table row with merchant_product_id: %s.'
                               % (dbt.table_name, merchant_product_id))
            cmd = "UPDATE %s SET " % dbt.table_name
            cmd += ", ".join(["%s=?" % dbc.column_name for dbc in ordered_columns])
            cmd += " WHERE merchant_product_id='%s'" % merchant_product_id
            # spider.logger.debug('Executing cmd: \n %s \n with data_tuple: %s' % (cmd, data_tuple))
            curs.execute(cmd, data_tuple)
            spider.logger.info('Updated %s table row for merchant_product_id: %s, '
                               % (dbt.table_name, merchant_product_id))
        else:
            # We have no row in products table with this merchant_product_id
            # Insert for first time so set `date_start` to 0.
            spider.logger.info('Attempt insert into %s table row for merchant_product_id: %s, '
                               % (dbt.table_name, merchant_product_id))
            cmd = "INSERT INTO %s (" % dbt.table_name
            cmd += ", ".join([dbc.column_name for dbc in ordered_columns])
            cmd += ") VALUES ("
            cmd += ", ".join(['?' for dbc in ordered_columns])
            cmd += ")"
            spider.logger.debug('Executing cmd: \n %s \n with data_tuple: %s' % (cmd, data_tuple))
            curs.execute(cmd, data_tuple)
            spider.logger.info('Inserted %s table row for merchant_product_id: %s'
                               % (dbt.table_name, merchant_product_id))

        # Other tables than master in order (e.g. a price table. There should always
        # be a product in the master with the merchant_product_id now)
        # We just do inserts for these rows (not updates). This means for prices
        # we end up with lots of price rows per product differing by data, so we can track
        # historic price of product etc
        for dbt in spider.target_site.dbtables.filter(master_table=False).order_by('order'):
            # price_options just temp holds the list of dicts
            ordered_columns = [dbc for dbc in dbt.dbcolumns.all().order_by('column_name')
                               if dbc.column_type != 'TIMESTAMP' and not dbc.primary_id_column
                               and str(dbc.column_name) != 'price_options']
            # Extra data (non-scraped) for this table
            fitem[dbt.table_name].update({'run_id': self.run_id, 'merchant_id': self.merchant_id,
                                          'merchant_product_id': merchant_product_id})
            # Build insert cmd for this table
            cmd = "INSERT INTO %s (" % dbt.table_name
            cmd += ", ".join([dbc.column_name for dbc in ordered_columns])
            cmd += ") VALUES ("
            cmd += ", ".join(['?' for dbc in ordered_columns])
            cmd += ")"

            if len(fitem[dbt.table_name].get('price_options', [])) == 0:
                # Insert default row if we don't have variants
                default_data = []
                non_null_data_cnt = 0
                for dbc in ordered_columns:
                    if dbc.column_name == 'variant_name':
                        default_data.append('default')
                    elif dbc.column_name.startswith('variant_'):
                        default_data.append(None)
                    else:
                        default_data.append(fitem[dbt.table_name][dbc.column_name])
                        if (dbc.column_name not in ['entry_id', 'merchant_product_id', 'run_id']
                           and fitem[dbt.table_name][dbc.column_name] is not None):
                            non_null_data_cnt += 1
                if non_null_data_cnt > 0:
                    # Always insert this row if non-null data, as it may contain other non default_ or variant_ data
                    spider.logger.debug('Inserting default row on table %s for merchant_product_id %s'
                                        % (dbt.table_name, merchant_product_id))
                    # spider.logger.debug('Executing cmd: \n %s \n with default data_tuple: %s' % (cmd, default_data))
                    curs.execute(cmd, tuple(default_data))
                    self.stats.inc_value('price_row_count')

            # Insert variant rows bulk insert
            variant_data_list = []
            for variant_option in fitem[dbt.table_name].get('price_options', []):
                # Build the data tuple for this variant_option
                variant_data = []
                # We will only do insert if at least one of variant cols are non null
                non_null_count = 0
                for dbc in ordered_columns:
                    if dbc.column_name == 'variant_name':
                        variant_data.append(variant_option['variant_name'])
                    elif dbc.column_name.startswith('variant_'):
                        if (variant_option.get(dbc.column_name) is not None):
                            non_null_count += 1
                            variant_data.append(variant_option[dbc.column_name])
                        else:
                            # We want an entry None even if we have no data for this column in fitem
                            variant_data.append(None)
                    else:
                        variant_data.append(fitem[dbt.table_name][dbc.column_name])
                if non_null_count > 0:
                    variant_data_list.append(variant_data)
            if variant_data_list:
                spider.logger.debug('Bulk Inserting variant rows on table %s for merchant_product_id: %s'
                                    % (dbt.table_name, merchant_product_id))
                # Need to look into bulk insert instead of this
                # spider.logger.debug('Executing cmd: \n %s \n with variant_data_list: %s' % (cmd, variant_data_list))
                curs.executemany(cmd, variant_data_list)
                self.stats.inc_value('price_row_count', len(variant_data_list))
