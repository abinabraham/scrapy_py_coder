# Collection of functions that will do various cleaning tasks etc when
# loading a Scrapy item, some are common to various spiders
from urlparse import urlparse, urlunparse
import re
import string
import logging
log = logging.getLogger(__name__)
from dateutil import parser
from scrapy.loader.processors import TakeFirst
take_first = TakeFirst()


def strip_punctuation(some_str):
    '''
    Remove all the punctuation (string.punctuation)
    from a string

    Args:
        some_str --- The string which will have its punctuation stripped
    Returns:
        the string with the punctuation removed
    '''
    return some_str.strip(string.punctuation)


def format_date_loader(date_str, loader_context):
    '''
    Take a date string, `date_str` and format it according to `output_format`.
    which should be set in the loader_context under the `datetime_format` key
    else, we take '%Y%m%d %H:%M:%S %p'` as default.

    See Python docs for format strings.
    The default format is the date format mysql seems to like.

    Args:
        date_str --- The string representing the date to be reformatted
        loader_context --- The item loader context
    Returns:
        The string representing the date in the new format
    '''
    output_format = loader_context.get('datetime_format', '%Y%m%d %H:%M:%S %p')
    if date_str:
        try:
            # Use dateutils parser (can handle almost anything)
            parsed_date = parser.parse(date_str)
            # Reformat according to output_format
            return parsed_date.strftime(output_format)
        except (ValueError, TypeError) as err:
            log.error('Error when reformatting date in loader:\n%s' % err)
    return date_str


def format_date(date_str, output_format='%Y-%m-%d'):
    '''
    Take a date string, `date_str` and format it according to `output_format`.
    See Python docs for format strings.
    The default format is the date format mysql seems to like.

    Args:
        date_str --- The string representing the date to be reformatted
    Kwargs:
        output_format --- The string representing the format that the date
                           should be formatted as
    Returns:
        The string representing the date in the new format
    '''
    if date_str:
        try:
            # Use dateutils parser (can handle almost anything)
            parsed_date = parser.parse(date_str)
            # Reformat according to output_format
            return parsed_date.strftime(output_format)
        except (ValueError, TypeError) as err:
            log.error('Error when reformatting date in loader:\n%s' % err)
    return date_str


def clean_uni(value):
    '''
    Strip all unicode from a string
    N.B. Don't confuse this for the builtin `unicode.strip`, which
    just trims a unicode strip

    N.B. This should not be needed if TDS Version > 7.0 is actually used
    and working. But could still be useful when extracting numeric data from string
    with regex

    The reason we encode to utf8 and decode to unicode escape before, instead
    of just directly encoding to ascii with 'ignore', is so that we also get rid of
    escapes unicode chars, e.g. u'\\xe8', encoding keeps it the same, but then decoding
    with 'unicode_escape' gives us an non-escaped uni char, u'\xe8', which after
    ascii ignore ignoring vanishes.

    Args:
        value --- The string which is to be stripped of unicode
    Returns:
        The string with unicode removed
    '''
    if value:
        return value.encode('utf8').decode('unicode_escape').encode('ascii', 'ignore').strip()
    return None


def clean_url(url):
    '''
    Ensure that the url has a scheme.

    NB. We could extend this to remove query string parameters etc if desired.

    Args:
        url --- String representing the URL to be cleaned
    Returns:
        If the url had no scheme, we will set `http` and then return the url
        with this scheme, else just returns the original url
    '''
    try:
        o = urlparse(url)
        if not o.scheme:
            # Add scheme if non existant
            o = o._replace(scheme='http')
        return(urlunparse(o))
    except AttributeError:
        return url


def get_dec(price_string, loader_context=None):
    '''
    Extract a currency, e.g. 1.5, 100.49, 1,250.69 etc.
    There are a wide variety of price formats we must match

    If needed we could use loader_context to extract different
    formats, i.e. set
    currency on a per spider basis?
    http://doc.scrapy.org/en/latest/topics/loaders.html#item-loader-context

    Should be set in the spider like
    loader = ProductLoader(...)
    loader.context['spider_currency'] = 'GBP'

    Args:
        price_string --- The string we extracted that should contain within in
                         somewhere the price, e.g. "15.10 GBP"
    Kwargs:
        loader_context --- It may be useful in the future to use the context
                            to set different formats depending on if GBP/US/EUR
                            For now it remains as an option
    Returns:
        A float extracted for the price or None if we couldn't extract one
    '''
    if price_string is None:
        return price_string

    # Decide what to do with currency later
    # spider_currency = loader_context.get('spider_currency', 'GBP')

    # First some preliminary cleaning/normalization to make the task easier
    # Although, if you run through `clean_uni` first it should have already taken care of \xa3 etc
    # Clean Unicode first should have take care of \xa3 etc
    # The reason we encode to utf8 and decode to unicode escape before, instead
    # of just directly encoding to ascii with 'ignore', is so that we also get rid of
    # escapes unicode chars, e.g. u'\\xe8', encoding keeps it the same, but then decoding
    # with 'unicode_escape' gives us an non-escaped uni char, u'\xe8', which after
    # ascii ignore ignoring vanishes.
    price_string = price_string.encode('utf8').decode('unicode_escape').encode('ascii', 'ignore').strip()
    price_string = price_string.replace(',', '')  # Get rid of any commas if thousands
    price_string = re.sub(r'\\x[ab]\d{1}', '', price_string)  # Get rid of a fair few utf8 currency symbols like \xa3
    # Replace any numbers formatted like 1-30 as 1.30 (doesn't remove -at start)
    price_string = re.sub(r'(\d+)\-(\d+)', r'\1.\2', price_string)

    # If ends in 'p' like '50p' then reformat it as '0.50'
    pence_regex = re.compile(r"^(?P<pence>\d+)p$")
    m = pence_regex.match(price_string)
    if m is not None:
        pence = take_first(m.groups())
        if pence is not None:
            price_string = str(float(pence)/100)

    # Extract the price
    regex = re.compile(r"\d+(?:\.\d{1,2}){0,}")
    price = take_first(regex.findall(price_string))
    if price:
        try:
            return float(price)
        except Exception:
            return None
    return None


def get_integer(number_string):
    '''
    Get the first integer found from a string

    N.B. Unicode like `\xa3` should have been removed first with `clean_uni`

    Args:
        number_string --- A string which may contain an integer somewhere
    Returns:
        The first integer found in the `number_string`
        or if one isn't found then zero is returned
    '''
    if number_string:
        regex = re.compile(r"\d+")
        count = take_first(regex.findall(number_string))
        if count:
            try:
                return int(count)
            except Exception:
                return 0
        return 0


def get_room_size(room_size):
    '''
    For Booking.com, the room size extracted comes with
    units of m2 or ft2 (unicode squared). We strip that and extract
    the numeric value.

    Args:
        room_size --- The string representing the room size that we extracted
                      along with units
    Returns:
        The numberic value of the room size
    '''
    if room_size:
        # Strip the units
        room_size = room_size.replace(u'm\xb2', '').replace(u'ft\xb2', '').strip()
        # Extract the numeric value
        regex = re.compile(r"\d+")
        return take_first(regex.findall(room_size))
    # Want to return None rather than '' or [] if not room_size
    return None


class TakeFirstSafe(object):

    '''
    Like TakeFirst but return empty string
    instead of None in the case when the list doesn't contain any non null objects
    This can be safely written to db

    The __call__ special method is the function ran when any instances of
    class are called in the manner of a function, e.g. a=A(), a()
    '''
    def __call__(self, values):
        '''
        Args:
            self --- The instance
            values --- Iterable from which the first non None and non empty string
                    object is to be returned
        Returns:
            The first non-None and non-empty string value from the iterable
            `values` else the empty string, ""
        '''
        for value in values:
            if value is not None and value != '':
                return value
        return ""

# Init an instance of TakeFirstSafe class, which can be called like a func
take_first_safe = TakeFirstSafe()


def parse_conditions(conditions):
    '''
    For Booking.com.
        > The conditions are stripped and the title (e.g. "Meal plans:") is
          joined with the body (e.g "Breakfast is included) to form a single
          string
        > Each condition is stripped of any unicode.
        > If 'cancellation before' in condition string, condition is replaced by 'Free Cancellation'
          (N.B. Without doing this we'd have multiple cond rows in conditions table
           for a given room differing by this cancellation date over the checkin dates...)
        > Each non-empty condition is added to a new list, `cleaned_conditions`
        > Duplicates are removed from the new list using sets
        > The cleaned conditions are joined together into a single string sep by '|'

    Args:
        conditions --- A list of Scrapy selectors representing booking condition
                       para elements
    Returns:
        A single string of cleaned conditions joined by '|' or None
    '''
    joined_conditions = []
    for cond in conditions:
        # Combine title and content of condition into single condition
        joined_conditions.append(' '.join([c.strip() for c
                                           in cond.xpath('.//text()').extract() if c.strip()]))

    cleaned_conditions = []
    for cond in joined_conditions:
        # cond = clean_uni(cond)
        if cond:
            # Strip out the date in free cancellation condition to
            # avoid multiple conds only differing by this date
            if 'cancellation before' in cond:
                cond = 'Free Cancellation'
            # Append to cleaned list
            cleaned_conditions.append(cond)
    # Remove duplicates
    cleaned_conditions = list(set(cleaned_conditions))
    if cleaned_conditions != []:
        return '|'.join(cleaned_conditions)
    # Want None if no conds, not [] or ''
    return None


def parse_amenities(amenities):
    '''
    For Booking.com, amenities will come as a list of comma sep strings.
        > Split these strings (by ',') into a list of sublists.
        > Flatten this list, keeping non-empty strings only
        > Duplicates are removed using sets
        > Amenities are returned as a single string, sep by '|'
    Args:
        amenities --- A list of comma separated strings
    Returns:
        A single string representing all ammenities, each separated by the delimiter
        '|' or None
    '''
    if amenities:
        amenities_parsed = [fac.split(',') for fac in amenities]
        # Flatten list keeping only the non-empty one after they're stripped
        flat_amenities = [item.strip() for sublist in amenities_parsed
                          for item in sublist if item.strip()]
        # Remove duplicates
        flat_amenities = list(set(flat_amenities))
        # Return conditions in '|' sep string for storage in SQL db.
        if flat_amenities:
            return '|'.join(flat_amenities).strip()
    # Want to return None for SQL, not '' or [] in non-existant case
    return None


class CleanTarrifList(object):
    '''
    For italotreno, make sure the tarrif_dicts in tarrif_list have all necessary attributes,
    and clean and parse these attributes accordingly.
    '''
    def __call__(self, tarrif_list):
        '''
        First check that each of the dicts have all the required
        data, else we ignore it and note error in log.
        For each tarrif with the required data, we parse and clean it
        and add it as a new dictionary to a new `cleaned_tarrif_list` list, to be returned.

        Args:
            self --- The CleanTarrifList instance
            tarrif_list --- A list of dictionaries representing data we scraped
        '''
        cleaned_tarrif_list = []
        for t in tarrif_list:
            service_name = t.get('t_service', None)
            offer_name = t.get('t_class', None)
            offer_price = t.get('t_price', None)
            # Check all required attributes are present
            if service_name is not None and offer_name is not None and offer_price is not None:
                try:
                    # Reformat price to use period instead of comma so get_dec
                    # can handle it
                    offer_price = get_dec(clean_uni(offer_price.replace(',', '.')))
                    cleaned_tarrif_list.append({'service_name': service_name, 'offer_name': offer_name,
                                                'offer_price': offer_price,
                                                })
                except (AttributeError, ValueError) as err:
                    log.error('The tarrif %r will be dropped as one or'
                              ' more invalid attribs:\n %s' % (t, err))
                    continue

        return cleaned_tarrif_list


class CleanServiceOffers(object):
    '''
    For trenitalia, make sure the serviceoffers in serviceofferslist have all necessary attributes,
    and clean and parse these attributes accordingly.
    '''
    def __call__(self, serviceoffers):
        '''
        First check that each of the dicts have all the required
        data, else we ignore it and note error in log.
        For each service offer with the required data, we parse and clean it
        and add it as a new dictionary to a new `cleaned_serviceoffers` list, to be returned.

        Args:
            self --- The CleanServiceOffers instance
            serviceoffers --- A list of dictionaries representing data we scraped
        '''
        cleaned_serviceoffers = []
        for so in serviceoffers:
            service_name = so.get('service_name', None)
            offer_name = so.get('offer_name', None)
            # offer_available = so.get('offer_available', None)
            offer_price = so.get('offer_price', None)
            leg_info = so.get('leg_info', None)
            train = so.get('train', None)
            # Check all required attributes are present
            # if service_name is not None and offer_name is not None and offer_available is not None and offer_price is not None:
            if service_name is not None and offer_name is not None and offer_price is not None and leg_info is not None and train is not None:
                try:
                    offer_price = get_dec(clean_uni(offer_price.replace(',', '.')))
                    train = clean_uni(train).replace('\n', '').strip()
                    # offer_available = int(offer_available)
                    legdeparturestation, legarrivalstation = [x.strip() for x in leg_info.replace(u'\xa0', '').split('\n')
                                                              if x.strip() and x.strip() != 'da' and x.strip() != 'a']
                    cleaned_serviceoffers.append({'service_name': service_name, 'offer_name': offer_name,
                                                  'legdeparturestation': legdeparturestation, 'legarrivalstation': legarrivalstation,
                                                  'offer_price': offer_price, 'train': train
                                                  })
                except (AttributeError, ValueError) as err:
                    log.error('The serviceoffer %r will be dropped as one or'
                              ' more invalid attribs:\n %s' % (so, err))
                    continue
            else:
                log.error('Dropped service offer as missing attribs: %s' % so)

        return cleaned_serviceoffers


class CleanLegs(object):
    '''
    For trenitalia, make sure the legs have all necessary attributes,
    and clean and parse these attributes accordingly.
    '''
    def __call__(self, legs):
        '''
        First check that each of the dicts have all the required
        data, else we ignore it and note error in log.
        For each leg with the required data, we parse and clean it
        and add it as a new dictionary to a new `cleaned_legs` list, to be returned.

        Args:
            self --- The CleanLegs instance
            legs --- A list of dictionaries representing data we scraped
        '''
        cleaned_legs = []
        if not legs:
            return []
        for leg in legs:
            leg_arrival_st = leg.get('leg_arrival_st', None)
            leg_arrival_time = leg.get('leg_arrival_time', None)
            leg_dep_st = leg.get('leg_dep_st', None)
            leg_dep_time = leg.get('leg_dep_time', None)
            # Check all required attributes are present
            if leg_arrival_st is not None and leg_arrival_time is not None and leg_dep_st is not None and leg_dep_time is not None:
                try:
                    leg_arrival_st = clean_uni(leg_arrival_st).strip()
                    leg_dep_st = clean_uni(leg_dep_st).strip()
                    # This signifies next day times
                    leg_arrival_time = leg_arrival_time.replace('*', '')
                    leg_dep_time = leg_dep_time.replace('*', '')
                    cleaned_legs.append({'leg_arrival_st': leg_arrival_st,
                                         'leg_arrival_time': leg_arrival_time,
                                         'leg_dep_st': leg_dep_st,
                                         'leg_dep_time': leg_dep_time})
                except (AttributeError, ValueError) as err:
                    log.error('The leg %r will be dropped as one or'
                              ' more invalid attribs:\n %s' % (leg, err))
                    continue
            else:
                log.error('Dropped leg as missing attribs: %s' % leg)

        return cleaned_legs


class CleanRooms(object):
    '''
    For Booking.com, make sure the room dictionaries in the list have all necessary attributes,
    and clean and parse these attributes accordingly.
    '''
    def __call__(self, rooms):
        '''
        First check that each of the rooms dicts have all the required
        data, else we ignore that room and note error in log.
        For each room with the required data, we parse and clean it
        and add it as a new dictionary to a new `cleaned_rooms` list, to be returned.

        Args:
            self --- The CleanRooms instance
            rooms --- A list of dictionaries representing data we scraped
                     from rooms of various type for a given listing.
        '''
        cleaned_rooms = []
        for room in rooms:
            desr = room.get('room_type_desr', None)
            ppl = room.get('room_type_max_ppl', None)
            date_booking = room.get('date_booking', None)
            pocket_price = room.get('pocket_price', None)
            full_price = room.get('full_price', None)
            room_stock = room.get('room_stock', None)
            amenities = room.get('room_type_amenities', None)
            room_size = room.get('room_size', None)
            room_conditions = room.get('room_conditions', None)
            # Check all required room attributes are present (not all rooms have full_price)
            if desr and ppl and date_booking and pocket_price and room_stock:
                try:
                    # Sometimes room desr contains unicode chars
                    # desr = clean_uni(desr.strip())
                    desr = desr.strip()
                    ppl = int(ppl)
                    date_booking = date_booking.strip()
                    pocket_price = get_dec(pocket_price)
                    full_price = get_dec(full_price)
                    room_stock = int(room_stock)
                    # For the list types we extracted, we must be careful to
                    # return None not [] if empty
                    room_conditions = parse_conditions(room_conditions)
                    room_size = get_room_size(''.join([rs_text.strip() for rs_text in room_size]))
                    amenities = parse_amenities(amenities)
                    cleaned_rooms.append({'room_type_desr': desr, 'room_type_max_ppl': ppl,
                                          'date_booking': date_booking, 'full_price': full_price,
                                          'pocket_price': pocket_price, 'room_stock': room_stock,
                                          'room_type_amenities': amenities, 'room_size': room_size,
                                          'room_conditions': room_conditions
                                          })
                except (AttributeError, ValueError) as err:
                    log.error('The room %r will be dropped as one or'
                              ' more invalid attribs:\n %s' % (room, err))
                    continue

        return cleaned_rooms


class RemovePlaceholders(object):
    '''
    Remove any img urls for placeholder imgs
    '''
    def __call__(self, values):
        good_vals = [val for val in values if 'transparent' not in val]
        return good_vals


def clean_ref(merchant_product_id):
    '''
    This should now really be achieved with regex in the config
    , but nevertheless if we see "Ref." remove it
    '''
    if merchant_product_id:
        return merchant_product_id.replace('Ref.', '')
    else:
        return ''


def min_non_zero(number_list):
    '''
    Get the min number from a least that is greater than 0
    Unless there isn't one, in which return zero
    '''
    # Filter out zero
    non_zero_number_list = [float(n) for n in number_list if float(n) > 0]
    # Return the min
    if non_zero_number_list:
        return min(non_zero_number_list)
    # If there are no positive numbers return 0
    return 0


def numeric_max(number_list):
    '''
    Get all numeric entries in list, then return
    the maximum of them. If there are none return zero
    '''
    floats = []
    # Only numeric entries
    for n in number_list:
        try:
            floats.append(float(n))
        except ValueError:
            pass
    # Return max float
    if floats:
        return max(floats)

    return 0.00


def remove_extra_dots(url):
    '''
    Sometimes we get url with structure
    http://www.example.com/../somewhere/
    These dots should be removed to rebuild the URL without
    '''
    parsed = list(urlparse(url))
    dirs = []
    for name in parsed[2].split("/"):
        if name == "..":
            if len(dirs) > 1:
                dirs.pop()
        else:
            dirs.append(name)
    parsed[2] = "/".join(dirs)
    return urlunparse(parsed)


def make_valid_url(url, loader_context):
    '''
    Check that a url has a netloc and a scheme
    Using reference_url in context to make absolute if not.
    '''

    reference_url = loader_context['reference_url']

    if reference_url is None:
        # No reference, return orig untouched
        return url
    else:
        reference_url = urlparse(reference_url)
        netloc, scheme = reference_url.netloc, reference_url.scheme
        if not scheme:
            # If reference url has no scheme, assume http and continue
            scheme = 'http'
        elif not netloc:
            # If no reference url provided we return original url untouched
            return url
    # Ensure valid URL falling back to reference URL vals if and when needed
    try:
        o = urlparse(url)
        if not o.scheme:
            # add scheme if non existant
            o = o._replace(scheme=scheme)
        if not o.netloc:
            o = o._replace(netloc=netloc)
        # Use urlunparse to reconstruct
        return(urlunparse(o))
    except AttributeError:
        return url
