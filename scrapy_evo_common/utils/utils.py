import re
import datetime
import string
import urllib
import json
import logging
from collections import OrderedDict
from urlparse import urlparse, urlunparse, parse_qsl, parse_qs
from w3lib.html import remove_tags
from scrapy.http import HtmlResponse
from scrapy.loader import ItemLoader
from scrapy.loader.processors import MapCompose, Compose
from scrapy.loader.processors import TakeFirst
take_first = TakeFirst()
logger = logging.getLogger(__name__)


class BadSelectorException(Exception):
    pass


class NoMatchException(Exception):
    pass


class NotUniqueException(Exception):
    pass


class BadFormatException(Exception):
    pass


SCRAPY_DEF_HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                      'Accept-Language': 'en-GB,en-US;q=0.8,en;q=0.6',
                      'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36',
                      'Accept-Encoding': 'gzip, deflate, sdch, br'
                      }


SCRAPY_XHR_HEADERS = {'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                      'Accept-Language': 'en',
                      'X-Requested-With': 'XMLHttpRequest',
                      'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/53.0.2785.143 Chrome/53.0.2785.143 Safari/537.36',
                      'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8'}


SCRAPY_JSON_XHR_HEADERS = {'Accept': 'application/json, text/javascript, */*; q=0.01',
                           'Accept-Language': 'en',
                           'X-Requested-With': 'XMLHttpRequest',
                           }


def _unquote(url):
    '''
    Sometimes the source_link we scraped was already URL encoded on the target page (particularly when unicode chars in URL).
    Our process of url quoting before passing back to the frontend in the "complete_url" during scraping the source_link
    in the listing phase and auto django unquoting when retriving the product detail URL from GET params in later detail phases
    does nothing to rid the source link scraped of this initial quoting (this portion just becomes intermediately double quoted).

    Python-requests doesn't like any URL quoating in URLs, so we must fully unquote it.

    This function ensures fully unquoted URL by continuing to unquote until no change occurs.

    Args:
        url - The URL to be fully unquoted
    '''
    n = urllib.unquote(url)
    if n == url:
        return url
    else:
        return _unquote(n)


def _quote_path(url):
    '''
    Given a byte str URL ensure that the path is fully
    percent encoded (this will include percent-encoding reserved symbols such
    as '?', ':', '%' , but by default excludes '/'. In this way quote is safe
    to use on the path component, but not the entire URL).

    Both requests and Scrapy don't quote these reserved chars, since if they
    did they would break the URL (i.e. http:// would go to http%3A//). See for example
    https://github.com/scrapy/w3lib/blob/master/w3lib/url.py#L73

    Nevertheless, some URLs contain reserved chars in the path that need to be quoted,
    for example BBR uses '%' to signify the ABV in the URLs of the whisky products, .e.g
    '/something-40%' and that percentage sign needs to be percent encoded itself.

    The encoding is utf8 by default, but should come from the response encoding
    that the URL was scraped from in general.
    '''
    if type(url) != str:
        raise TypeError('URL passed to quote path should be byte str')
    o = urlparse(url)
    # Avoid double percent encoding, by ensuring everything fully unquoted first
    path = _unquote(o.path)
    o = o._replace(path=urllib.quote(path))
    return urlunparse(o)


def do_sel_extraction(xpath, jsonpathstr, regex_pattern, variant_name, allow_multiple_results,
                      field_name, product_listing_xhr_response, page_type, test_response, test_url, trainer=True):

    if not (xpath or jsonpathstr):
        # Cant extract anything
        raise BadSelectorException('No XPATH or JSONPath for selector for'
                                   ' field_name %s' % field_name)

    # The datavals extracted to be returned
    all_datavals = []

    json_nodes = None
    if jsonpathstr and product_listing_xhr_response != 'X' and page_type == 'L':
        if isinstance(test_response, dict):
            jresponse = test_response
            try:
                from jsonpath_rw import parse
                jsonpath_expr = parse(jsonpathstr)
                json_nodes = [match.value for match in jsonpath_expr.find(jresponse)]
            except ValueError as e:
                raise ValueError('ValueError when attempting to parse JSON: %s' % str(e))
            if len(json_nodes) == 0:
                raise NoMatchException('JSONPATH has no matching nodes under chosen test_response!')
            elif len(json_nodes) > 1 and not allow_multiple_results:
                raise NotUniqueException('JSONPATH is not unique and allow multiple results not checked')
        else:
            raise BadFormatException('JSONPATH was provided and not X target,'
                                     ' yet product not JSON')

    if xpath:
        # Get the element corresponding to XPATH
        if json_nodes:
            for json_node in json_nodes:
                datavals = []
                # We got node via JSONPath, since XPATH specified too, we
                # assume this node under the JSON key must be XML and we are to
                # try to extract data from it via XPATH now
                if isinstance(json_node, basestring):
                    keyed_resp = HtmlResponse(url=test_url, body=json_node, encoding=test_response.encoding)
                    node_list = keyed_resp.xpath(xpath)
                else:
                    raise BadFormatException('XPATH was provided and we got JSON node'
                                             ' yet json node not basestring (not XML)'
                                             ' You should probably remove XPATH or modify JSONPath.')

                # Use the path to get the node of the product
                if regex_pattern:
                    datavals = node_list.re(regex_pattern)
                else:
                    datavals = node_list.extract()
                # Ensure we have a list
                if type(datavals) == tuple:
                    datavals = list(datavals)
                if type(datavals) != list:
                    datavals = [datavals, ]
                # Insert to all data vals
                all_datavals.extend(datavals)

        else:
            # We didn't get node from JSONPath
            if not isinstance(test_response, dict):
                node_list = test_response.xpath(xpath)
            else:
                raise BadFormatException('XPATH was provided and we didnt get JSON node first'
                                         ' yet product is a JSON dict.'
                                         ' You should probably add a JSONPath.')

            datavals = []
            # Use the path to get the node of the product
            if regex_pattern:
                datavals = node_list.re(regex_pattern)
            else:
                datavals = node_list.extract()
            # Ensure we have a list
            if type(datavals) == tuple:
                datavals = list(datavals)
            if type(datavals) != list:
                datavals = [datavals, ]
            # Insert to all_datavals
            all_datavals.extend(datavals)
    else:
        # If not xpath, the node extracted by the JSONPath itself should be
        # the data of interest
        for json_node in json_nodes:
            if isinstance(json_node, basestring):
                datavals = []
                if regex_pattern:
                    match = re.match(regex_pattern, json_node)
                    if match:
                        datavals = match.groups()
                else:
                    datavals = [json_node, ]
                # Sometimes dataval is tuple other times a string
                if type(datavals) == tuple:
                    datavals = list(datavals)
                if datavals is not None and len(datavals) > 0:
                    all_datavals.extend(datavals)

            else:
                # If multiple sources for field extend list
                if isinstance(json_node, list):
                    all_datavals.extend(json_node)
                else:
                    all_datavals.append(json_node)
    # Finished
    if len(all_datavals) > 1 and not allow_multiple_results:
        if trainer:
            # Exception appropriate to show in the trainer
            raise NotUniqueException('This selector matches %i results. It is not unique'
                                     ' and allow multiple results not checked' % len(datavals))
        else:
            # Exception with more info for spider log
            raise NotUniqueException('This selector matches %i results. It is not unique'
                                     ' and allow multiple results not checked. Skipping. Datavals were: %s'
                                     % (len(datavals), datavals))
    return all_datavals


def _build_loader_func(code_string):
    '''
    Take the user's custom input code_string and build a function object from it

    Args:
        code_string --- The custom input user code string
    Returns:
        A namespace containing the function object

    N.B. This function may raise a SyntaxError, so the calling code should
    be ready to catch and deal with that. For null or empty code_string args
    this function will return None, so the calling code should also deal with that case.

    N.B. the raw python func is created by safely using exec with a
    limited scope: `exec(buildFuncStr(codestr), {"__builtins__":None}, {})`
    HOWEVER, this is still not really that safe:
    http://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html
    , so access should be highly restricted to views with this form.
    '''
    if code_string is not None and len(code_string) > 0:
        namespace = {}
        exec(code_string, {"__builtins__": None, 're': re, 'urllib': urllib, 'urlparse': urlparse,
                           'urlunparse': urlunparse, 'int': int, 'string': string, 'str': str, 'float': float,
                           'unicode': unicode, 'max': max, 'min': min, 'list': list,
                           'remove_tags': remove_tags, 'take_first': take_first, 'type': type, 'len': len, 'ValueError': ValueError,
                           'True': True, 'False': False, 'set': set,
                           'datetime': datetime,
                           'sum': sum, 'Exception': Exception}, namespace)
        return namespace
    return None


def _build_loader_class(loaderpipelines, target_site, htmlselector_cls):
    '''
    Dynamically build the ProductLoader using the configuration
    generated in the trainer

    Args:
        loaderpipelines --- A set of LoaderPipeline objects that specify the input
                            and output Scrapy loader processors and the ordered
                            set of cleaning functions that the processor will compose/
                            map compose
        target_site --- The TargetSite instance corresponding to a given merchant
        htmlselector_cls --- The class for the HTMLSelector Django model (import
                            in spider to avoid issues with circ imports)

    Returns:
        A Scrapy Loader class
    '''
    # Defaults
    methods = {}
    methods['default_input_processor'] = MapCompose(remove_tags, unicode.strip)
    methods['default_output_processor'] = TakeFirst()
    # The pipelines for the various fields
    for lp in loaderpipelines:
        ptype = 'in' if lp.in_or_out == 'I' else 'out'
        processor_names = []
        # Table name and column name (field name of format <table_name>___<column_name>)
        dbt_name, dbc_name = lp.field_name.split('___')
        if dbc_name.startswith('variant_'):
            # Special columns prefixed with "variant_" are to store data for product variants
            # We need to add a processor for each of the distinct variants, whose data will be stored
            # in fields in the Scrapy Item like `table_name___column_name___variant_name`

            # Selectors for this target matching field name
            hsels = htmlselector_cls.objects.filter(scrapy_field__field_name=lp.field_name,
                                                    scrapy_field__conf__target_site=target_site)
            # Exclude anything with null or empty string variant name
            hsels = hsels.exclude(variant_name="").exclude(variant_name__isnull=True)
            # All distinct variant names
            variant_names = set([hsel.variant_name for hsel in hsels])
            for variant_name in variant_names:
                processor_names.append('%(field_name)s___%(variant_name)s_%(ptype)s'
                                       % {'field_name': lp.field_name,
                                          'ptype': ptype, 'variant_name': variant_name})
        else:
            processor_names.append('%(field_name)s_%(ptype)s'
                                   % {'field_name': lp.field_name,
                                      'ptype': ptype})

        # Now build and compose the functions in order
        loader_funcs = lp.members.all().order_by('position__pipeorder')
        func_list = []
        for lf in loader_funcs:
            # Syntax should have been validated by form
            namespace = _build_loader_func(lf.func_definition)
            func_name, func_obj = namespace.popitem()
            func_list.append(func_obj)
        # If no member functions of the pipeline we don't add it and the
        # defaults will be used.
        if func_list:
            for pname in processor_names:
                if lp.composition == 'C':
                    methods[pname] = Compose(*func_list)
                else:
                    methods[pname] = MapCompose(*func_list)

    # Build and return the dynamically constructed ProductLoader class
    return type('ProductLoader', (ItemLoader, ), methods)


def _build_terminate_paging_func(code_string):
    '''
    Take the user's custom input code_string (in which they should define
    a Boolean variable 'terminate` using the scraped vars accessible to them via
    kwargs because of the way terminate_paging_func will be called) and build a function object from it

    Args:
        code_string --- The custom input user code string
    Returns:
        A function object called `terminate_paging_func`

    N.B. This function may raise a SyntaxError, so the calling code should
    be ready to catch and deal with that. For null or empty code_string args
    this function will return None, so the calling code should also deal with that case.

    N.B. the raw python func is created by safely using exec with a
    limited scope: `exec(buildFuncStr(codestr), {"__builtins__":None}, {})`
    HOWEVER, this is still not really that safe:
    http://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html
    , so access should be highly restricted to views with this form.
    '''
    if code_string is not None and len(code_string) > 0:
        loc = code_string.splitlines()
        code_string = '\n\t'.join(loc)  # Add in tabs and newlines to keep indentation correct
        namespace = {}   # The namespace function will be defined in
        func_str = 'def terminate_paging_func(*args, **kwargs):'
        func_str += '\n\tterminate = None'
        func_str += '\n\t' + code_string
        func_str += '\n\treturn terminate'
        # Defines ppf func (attempt to make a little safer, although note not entirely)
        exec(func_str, {"__builtins__": None, 're': re, 'urllib': urllib,
                        'int': int, 'str': str, 'False': False, 'True': True,
                        'len': len}, namespace)
        return namespace['terminate_paging_func']
    return None


def _build_next_link_func(code_string):
    '''
    Take the user's custom input code_string (in which they should define
    a variable 'next_link` using the scraped vars accessible to them via
    kwargs because of the way build_next_link_func will be called)
    and build a function object from it

    Args:
        code_string --- The custom input user code string
    Returns:
        A function object called `build_next_link_func`

    N.B. This function may raise a SyntaxError, so the calling code should
    be ready to catch and deal with that. For null or empty code_string args
    this function will return None, so the calling code should also deal with that case.

    N.B. the raw python func is created by safely using exec with a
    limited scope: `exec(buildFuncStr(codestr), {"__builtins__":None}, {})`
    HOWEVER, this is still not really that safe:
    http://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html
    , so access should be highly restricted to views with this form.
    '''
    if code_string is not None and len(code_string) > 0:
        loc = code_string.splitlines()
        code_string = '\n\t'.join(loc)  # Add in tabs and newlines to keep indentation correct
        namespace = {}   # The namespace function will be defined in
        func_str = 'def build_next_link_func(*args, **kwargs):'
        func_str += '\n\tnext_link = None'
        func_str += '\n\t' + code_string
        func_str += '\n\treturn next_link'
        # Defines ppf func (attempt to make a little safer, although note not entirely)
        exec(func_str, {"__builtins__": None, 're': re, 'urllib': urllib,
                        'int': int, 'str': str, 'True': True, 'False': False}, namespace)
        return namespace['build_next_link_func']
    return None


def _build_payload_func(code_string):
    '''
    Take the user's custom input code_string (in which they should define
    a variable `payload` as a dictionary using the endpoint and qparams accessible to them via
    kwargs because of the way plf will be called) and builds a function object from it

    Args:
        code_string --- The custom input user code string
    Returns:
        A function object called `plf` (payload function)

    N.B. This function may raise a SyntaxError, so the calling code should
    be ready to catch and deal with that. For null or empty code_string args
    this function will return None, so the calling code should also deal with that case.

    N.B. the raw python func is created by safely using exec with a
    limited scope: `exec(buildFuncStr(codestr), {"__builtins__":None}, {})`
    HOWEVER, this is still not really that safe:
    http://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html
    , so access should be highly restricted to views with this form.
    '''
    if code_string is not None and len(code_string) > 0:
        loc = code_string.splitlines()
        code_string = '\n\t'.join(loc)  # Add in tabs and newlines to keep indentation correct
        namespace = {}   # The namespace function will be defined in
        func_str = 'def plf(*args, **kwargs):'
        func_str += '\n\tpayload = None'
        func_str += '\n\t' + code_string
        func_str += '\n\tassert isinstance(payload,dict), "payload is not dict"'
        func_str += '\n\treturn payload'
        # Defines plf func (attempt to make a little safer, although note not entirely)
        exec(func_str, {"__builtins__": None, 're': re, 'urllib': urllib,
                        'int': int, 'str': str, 'isinstance': isinstance, 'dict': dict,
                        'OrderedDict': OrderedDict,
                        'urlparse': urlparse,
                        'urlunparse': urlunparse,
                        'parse_qs': parse_qs,
                        'parse_qsl': parse_qsl,
                        'list': list,
                        'set': set,
                        'json': json,
                        'AssertionError': AssertionError},
             namespace)
        return namespace['plf']
    return None


def _build_ppf(code_string):
    '''
    Take the user's custom input code_string (in which they should define
    a variable 'xhr_url` using the endpoint and qparams accessible to them via
    kwargs because of the way ppf will be called) and builds a function object from it

    Args:
        code_string --- The custom input user code string
    Returns:
        A function object called `ppf` (post processing function)

    N.B. This function may raise a SyntaxError, so the calling code should
    be ready to catch and deal with that. For null or empty code_string args
    this function will return None, so the calling code should also deal with that case.

    N.B. the raw python func is created by safely using exec with a
    limited scope: `exec(buildFuncStr(codestr), {"__builtins__":None}, {})`
    HOWEVER, this is still not really that safe:
    http://nedbatchelder.com/blog/201206/eval_really_is_dangerous.html
    , so access should be highly restricted to views with this form.
    '''
    if code_string is not None and len(code_string) > 0:
        loc = code_string.splitlines()
        code_string = '\n\t'.join(loc)  # Add in tabs and newlines to keep indentation correct
        namespace = {}   # The namespace function will be defined in
        func_str = 'def ppf(*args, **kwargs):'
        func_str += '\n\txhr_url = None'
        func_str += '\n\t' + code_string
        func_str += '\n\treturn xhr_url'
        # Defines ppf func (attempt to make a little safer, although note not entirely)
        exec(func_str, {"__builtins__": None, 're': re, 'urllib': urllib,
                        'int': int, 'str': str}, namespace)
        return namespace['ppf']
    return None


def _terminate_paging(scrapy_resp, paginationconf, product_listing_url, page_no=0):
    '''
    Use the user's custom terminate paging function to return a Boolean for
    whether pagination should cease.

    Args:
        scrapy_resp --- A Scrapy Response object
        paginationconf --- The custom pagination conf instance for this target
                           that the user defined
        product_listing_url --- Make this available to the user in their custom code
    Kwargs:
        page_no --- For pagination targets, this specifies which page we are on.
                    Certain vars will have the `pagination_increment` set
                    and we set them to `value + (page_no x pagination_increment)`
    Returns:
        A dictionary, with success key indicating if the build was a success or failure
        , an error key providing any error msgs and the terminate key providing the Boolean value
        of the terminate func built or True if it failed to build (default is to terminate)
    '''
    # Default is to terminate
    terminate = True
    error = None

    # User input function for termination of pagination
    try:
        tpf = _build_terminate_paging_func(paginationconf.terminate_paging_func)
    except SyntaxError:
        logger.error('The function built from your code has invalid syntax')
        return {'success': False, 'error': 'The function built from your code has invalid syntax',
                'terminate': True}

    if tpf:
        # Get the scraped variables
        var_dict = {}
        for var in paginationconf.scrapedvariables.all():
            var_dict[var.var_name] = u''  # Default
            var_value = var.var_value
            var_pagination_increment = var.pagination_increment
            var_multiple_vals = var.multiple_vals
            var_allow_absent = var.allow_absent
            # Uses the jsonpath, xpath, regex, line_number to extract a value for this var from resp
            xvarval = _extract_var_value(scrapy_resp, var.jsonpath, var.xpath,
                                         var.regex_pattern, var.line_number,
                                         var_multiple_vals, var_allow_absent,
                                         product_listing_url=product_listing_url)
            if var_value is not None and var_value is not u'':
                # User specifided static var value, use it as priority
                var_dict[var.var_name] = unicode(var_value)
            elif xvarval is not None:
                if not var_multiple_vals:
                    var_dict[var.var_name] = unicode(xvarval)
                else:
                    var_dict[var.var_name] = xvarval
            # If this is a pagination affected var, increment it accordingly
            if page_no > 0 and var_pagination_increment is not None and not var_multiple_vals:
                try:
                    var_dict[var.var_name] = int(var_dict[var.var_name]) + (int(var_pagination_increment) * int(page_no))
                    logger.debug('Incremented %s to: %s' % (var.var_name, var_dict[var.var_name]))
                except ValueError as verr:
                    # couldn't convert var_dict[var.var_name] to int (user entered non
                    # numeric value, or one was extracted, despite also having pg incr)
                    error = 'Error doing pagination incremenet: \n%s' % str(verr)
                    logger.error(error)
                    return {'success': False, 'error': error, 'terminate': True}

        # Now we use this dynamically gen function called tpf to determine
        # whether to terminate paging
        try:
            terminate = tpf(product_listing_url=product_listing_url, **var_dict)
            success = True
            # logger.debug('Used custom tpf to determine terminate as: %s' % terminate)
        except Exception as err:
            # Some exception with user code at runtime
            logger.error('Runtime error with user tpffunction: %s' % str(err))
            error = 'The function built from your code had runtime error'
            success = False

    logger.debug('Returning terminate:%s on product listing url %s and page_no %s'
                 % (terminate, product_listing_url, page_no))
    return {'success': success, 'terminate': terminate,
            'error': error}


def _extract_param_value(scrapy_resp, xpath, regex_pattern, line_number, multiple_vals, allow_absent):
    '''
    Using the xpath, regex_pattern and line_number
    of the instance of QParam passed, param, we attempt
    to extract a value for this query param from the response.

    Args:
        scrapy_resp --- A Scrapy Response object from which the parameter
                        will be attempted to be extracted
        xpath --- The xpath
        regex_pattern --- regex pattern
        line_number --- Line number in text extracted to focus on
        multiple_vals --- Does this parameter expect a list of values or single value?
        allow_absent --- Acceptable for this param to not be present?
    Returns:
        The extracted value(s) for this parameter or None if one couldn't be extracted
    '''

    if xpath is not None and xpath != u'':
        try:
            node_list = scrapy_resp.xpath(xpath)
        except ValueError as e:
            logger.error(e)
            return None
        if len(node_list) is 0 and not allow_absent:
            logger.error(u'No matching nodes in response for xpath %s and URL:%s with req headers: %s \n Response: %s'
                         % (xpath, scrapy_resp.url, scrapy_resp.request.headers, scrapy_resp.body.decode(scrapy_resp.encoding)))
            return None
        elif len(node_list) > 1 and not multiple_vals:
            logger.error('Xpath %s matches multiple (%i) nodes' % (xpath, len(node_list)))
            return None

        datavals = []
        for node in node_list:
            dataval = node.extract()
            # Sometimes dataval is list other times string
            if type(dataval) == list:
                dataval = take_first(dataval)
            # If the user specified line number we focus on that line only
            if line_number is not None:
                try:
                    dataval = dataval.splitlines()[line_number]
                except IndexError:
                    logger.error('The extracted value has only %i lines' % len(dataval.splitlines()))
                    continue
            # If the user specified a regex pattern, apply it
            if regex_pattern is not None and regex_pattern != u'':
                rgx = re.compile(regex_pattern)
                dataval = take_first(rgx.findall(dataval))
            # Check if all this yielded something sensible
            if dataval is None:  # or dataval.strip() == u'': (sometimes the value can be genuinely blank)
                valerr = u'For url: %s. No non-empty data was extracted for XPATH %s.' % (scrapy_resp.url, xpath)
                if regex_pattern:
                    valerr += u' Using regex:%s' % regex_pattern
                if line_number is not None:
                    valerr += u' And Using line_number:%s' % line_number
                logger.error(valerr)
                continue
            else:
                datavals.append(dataval)

        # Return extracted data
        if multiple_vals:
            return datavals
        else:
            return take_first(datavals)
    return None


def _build_xhr_url(scrapy_resp, lpxhrconf, product_listing_url=None, page_no=0):
    '''
    Using the scrapy resp and user defined xhr conf
    , build an XHR URL

    Args:
        scrapy_resp --- A Scrapy Response object
        lpxhrconf --- The Listing Page XHR Conf instance for this target
                     that the user defined
    Kwargs:
        page_no --- For pagination targets, this specifies which page we are on.
                    Certain parameters will have the `pagination_increment` set
                    and we set them to `value + (page_no x pagination_increment)`
    Returns:
        A dictionary, with success key indicating if the build was a success or failure
        , an error key providing any error msgs and an xhr_url key providing the value
        of the XHR URL built or None if it failed to be built
    '''
    # Init some defaults
    xhr_url = None
    error = None
    success = False
    ppf = None

    # Endpoint
    endpoint = lpxhrconf.endpoint
    # User input function for construction of XHR URL
    try:
        ppf = _build_ppf(lpxhrconf.post_processing_func)
    except SyntaxError:
        logger.error('The function built from your code has invalid syntax')
        return {'success': False, 'error': 'The function built from your code has invalid syntax',
                'xhr_url': None}

    # Params in ordered dict
    param_dict = OrderedDict()
    for param in lpxhrconf.qparams.all():
        param_dict[param.param_name] = u''   # Default
        param_value = param.param_value
        param_pagination_increment = param.pagination_increment
        param_multiple_vals = param.multiple_vals
        param_allow_absent = param.allow_absent
        # Uses the xpath, regex, line_number to extract a value for this param from resp
        xparamval = _extract_param_value(scrapy_resp, param.xpath, param.regex_pattern, param.line_number, param_multiple_vals, param_allow_absent)
        if param_value is not None and param_value is not u'':
            # User specifided static param value, use it as priority
            param_dict[param.param_name] = unicode(param_value)
        elif xparamval is not None:
            if not param_multiple_vals:
                param_dict[param.param_name] = unicode(xparamval)
            else:
                param_dict[param.param_name] = xparamval
        # If this is a pagintion affected parameter increment it accordingly
        if page_no > 0 and param_pagination_increment is not None and not param_multiple_vals:
            try:
                param_dict[param.param_name] = int(param_dict[param.param_name]) + (int(param_pagination_increment) * int(page_no))
                logger.debug('Incremented %s param to: %s' % (param.param_name, param_dict[param.param_name]))
            except ValueError as verr:
                # couldn't param_dict[param.param_name] to int (user entered non
                # numeric value, or one was extracted, despite also having pg incr)
                error = 'Error doing pagination incremenet: \n%s' % str(verr)
                logger.error(error)
                return {'success': False, 'error': error, 'xhr_url': None}

    # Build resulting XHR
    endpoint = lpxhrconf.endpoint
    if ppf:
        # Now we use this dynamically gen function called ppf to gen xhr
        try:
            xhr_url = ppf(product_listing_url=product_listing_url, endpoint=endpoint, **param_dict)
            success = True
            # logger.debug('Used custom ppf to build XHR: %s' % xhr_url)
        except Exception as err:
            # Some exception with user code at runtime
            logger.error('Runtime error with user ppf function: %s' % str(err))
            error = 'The function built from your code had runtime error'
            success = False
    else:
        # Build a default
        logger.debug('Using default serialization to build XHR...')
        encoding = 'utf8'  #
        encoded_param_dict = {k.encode(encoding): v.encode(encoding) for k, v in param_dict.items()}
        xhr_url = endpoint + '?' + urllib.urlencode(encoded_param_dict)
        success = True

    # logger.debug('Returning xhr_url:%s' % xhr_url)
    return {'success': success, 'xhr_url': xhr_url,
            'error': error}


def _build_payload(scrapy_resp, lpxhrconf, page_no=0, product_listing_url=None):
    '''
    Using the scrapy resp and user defined xhr conf
    , build a payload dict

    Args:
        scrapy_resp --- A Scrapy Response object
        lpxhrconf --- The Listing Page XHR Conf instance for this target
                      that the user defined
    Kwargs:
        page_no --- For pagination targets, this specifies which page we are on.
                    Certain parameters will have the `pagination_increment` set
                    and we set them to `value + (page_no x pagination_increment)`
    Returns:
        A dictionary, with success key indicating if the build was a success or failure
        , an error key providing any error msgs and an xhr_url key providing the value
        of the payload built or None if it failed to be built
    '''
    # Init some defaults
    payload = None
    error = None
    success = False
    plf = None

    # Endpoint
    endpoint = lpxhrconf.endpoint
    # User input function for construction of XHR URL
    try:
        plf = _build_payload_func(lpxhrconf.payload_func)
    except SyntaxError:
        logger.error('The function built from your code has invalid syntax')
        return {'success': False, 'error': 'The function built from your code has invalid syntax',
                'payload': None}

    # Params in ordered dict
    param_dict = OrderedDict()
    for param in lpxhrconf.qparams.all():
        param_dict[param.param_name] = u''   # Default
        param_value = param.param_value
        param_pagination_increment = param.pagination_increment
        param_multiple_vals = param.multiple_vals
        param_allow_absent = param.allow_absent
        # Uses the xpath, regex, line_number to extract a value for this param from resp
        xparamval = _extract_param_value(scrapy_resp, param.xpath, param.regex_pattern, param.line_number, param_multiple_vals, param_allow_absent)
        if param_value is not None and param_value is not u'':
            # User specifided static param value, use it as priority
            param_dict[param.param_name] = unicode(param_value)
        elif xparamval is not None:
            if not param_multiple_vals:
                param_dict[param.param_name] = unicode(xparamval)
            else:
                param_dict[param.param_name] = xparamval
        # If this is a pagintion affected parameter increment it accordingly
        if page_no > 0 and param_pagination_increment is not None and not param_multiple_vals:
            try:
                param_dict[param.param_name] = int(param_dict[param.param_name]) + (int(param_pagination_increment) * int(page_no))
                logger.debug('Incremented %s param to: %s' % (param.param_name, param_dict[param.param_name]))
            except ValueError as verr:
                # couldn't param_dict[param.param_name] to int (user entered non
                # numeric value, or one was extracted, despite also having pg incr)
                error = 'Error doing pagination incremenet: \n%s' % str(verr)
                logger.error(error)
                return {'success': False, 'error': error, 'payload': None}

    # Build resulting payload
    endpoint = lpxhrconf.endpoint
    if plf:
        # Now we use this dynamically gen function called ppf to gen xhr
        try:
            payload = plf(endpoint=endpoint, product_listing_url=product_listing_url, **param_dict)
            success = True
            # logger.debug('Used custom ppf to build XHR: %s' % payload)
        except Exception as err:
            # Some exception with user code at runtime
            logger.error('Runtime error with user plf function: %s' % str(err))
            error = 'The function built from your code had runtime error'
            success = False
    else:
        # Build a default
        payload = {}

    return {'success': success, 'payload': payload,
            'error': error}


def _extract_var_value(scrapy_resp, jsonpathstr, xpath, regex_pattern, line_number, multiple_vals, allow_absent, product_listing_url=None):
    '''
    Using the jsonpath, xpath, regex_pattern and line_number
    of the instance of ScrapedVariable (associated custom pagination conf override)
    passed, we attempt to extract a value for this scraped var from the response.

    Args:
        scrapy_resp --- A Scrapy Response object from which the parameter
                        will be attempted to be extracted
        jsonpathstr --- The JSONPath
        xpath --- The xpath
        regex_pattern --- regex pattern
        line_number --- Line number in text extracted to focus on
        multiple_vals --- Expect list of vals?
        allow_absent --- Acceptable for this var to be not be present?
    Kwargs:
        product_listing_url --- The original product listing URL, useful for debugging
    Returns:
        The extracted value for this var or None if one couldn't be extracted
    '''
    # Need scrapy_resp to be non-zero
    if scrapy_resp is None or scrapy_resp.body is None or scrapy_resp.body == '':
        return None

    # Can't extract unless at least one of these is non-null
    if jsonpathstr is None and xpath is None:
        return None

    jmatch_vals = []
    if jsonpathstr:
        from jsonpath_rw import parse
        jsonpath_expr = parse(jsonpathstr)
        try:
            jscrapy_resp = json.loads(scrapy_resp.body)
        except ValueError as verr:
            logger.error(u'Response with url %s and product listing URL %s is possibly non-JSON.'
                         ' We got error \n%s'
                         ' \n and body is \n%s\n'
                         ' You should not specify JSONPATH for non-JSON'
                         % (scrapy_resp.url, product_listing_url, verr, scrapy_resp.body.decode(scrapy_resp.encoding)[:100]))
            return None
        jmatch_vals = [match.value for match in jsonpath_expr.find(jscrapy_resp)]

    datavals = []
    if xpath is not None and xpath != u'':
        if jmatch_vals:
            # If json match and also xpath specified, assume the match is xml
            # and we should attempt to extract data from it with xpath
            # First use the value to build scrapy response.
            try:
                scrapy_resps = [HtmlResponse(url=scrapy_resp.url, body=j_val, encoding=scrapy_resp.encoding)
                                for j_val in jmatch_vals]
            except ValueError as verr:
                logger.error('XPATH specified yet JSON values extracted'
                             ' are not XML. Remove XPATH or adjust JSONPath:\n%s'
                             % str(verr))
                return None
        else:
            # Not a JSONpath so we assume response is XML and apply xpath directly
            scrapy_resps = [scrapy_resp, ]

        node_list = [sr.xpath(xpath) for sr in scrapy_resps]
        node_list = [item for sublist in node_list for item in sublist]  # Flatten
        if len(node_list) is 0 and not allow_absent:
            logger.error(u'No matching nodes in response for XPATH:%s'
                         ' for url %s and for jmatch_val %s, jsonpathstr %s,'
                         ' product listing URL %s and scrapy_resp:\n%s'
                         ' \n With user agent:\n%s \n and headers: \n %s'
                         % (xpath, scrapy_resp.url, jmatch_vals, jsonpathstr,
                            product_listing_url, scrapy_resp.body.decode(scrapy_resp.encoding),
                            scrapy_resp.request.meta.get('User-Agent'), scrapy_resp.request.headers))
            return None
        elif len(node_list) > 1 and not multiple_vals:
            logger.error('XPATH %s matches multiple (%i) nodes'
                         ' yet multiple values not checked'
                         % (xpath, len(node_list)))
            return None

        for node in node_list:
            # Now extract values
            dataval = node.extract()
            # Sometimes dataval is list other times string
            if type(dataval) == list:
                dataval = take_first(dataval)
            # If the user specified line number we focus on that line only
            if line_number is not None:
                try:
                    dataval = dataval.splitlines()[line_number]
                except IndexError:
                    logger.error('The extracted value has only %i lines' % len(dataval.splitlines()))
                    continue
            # If the user specified a regex pattern, apply it
            if regex_pattern is not None and regex_pattern != u'':
                rgx = re.compile(regex_pattern)
                dataval = take_first(rgx.findall(dataval))
            # Check if all this yielded something sensible
            if dataval is None:  # or dataval.strip() == u'': (sometimes the value can be genuinely blank)
                valerr = 'For url: %s. No non-empty data was extracted for XPATH %s.' % (scrapy_resp.url, xpath)
                if regex_pattern:
                    valerr += ' Using regex:%s' % regex_pattern
                if line_number is not None:
                    valerr += ' And Using line_number:%s' % line_number
                logger.error(valerr)
                continue
            else:
                datavals.append(dataval)
    else:
        # No XPATH so just assume straight JSON response
        for jmatch_val in jmatch_vals:
            if isinstance(jmatch_val, basestring):
                dataval = None
                if regex_pattern:
                    match = re.match(regex_pattern, jmatch_val)
                    if match:
                        dataval = match.groups()
                else:
                    dataval = jmatch_val
                # Sometimes dataval is list other times a string
                if type(dataval) == tuple:
                    dataval = take_first(dataval)
                if dataval is None or dataval.strip() == u'':
                    valerr = 'No non-empty data was extracted for JSONPath %s.' % xpath
                    if regex_pattern:
                        valerr += ' Using regex:%s' % regex_pattern
                    logger.error(valerr)
                    continue
                else:
                    datavals.append(dataval)
            else:
                datavals.append(jmatch_val)

    # Return
    if multiple_vals:
        return datavals
    else:
        return take_first(datavals)


def _build_next_link(scrapy_resp, paginationconf, product_listing_url=None, page_no=0):
    '''
    Using the scrapy resp and user defined pagination conf build the pagination
    next link (this forms part of the pagination override system)

    Args:
        scrapy_resp --- A Scrapy Response object
        paginationconf --- The custom pagination conf instance for this target
                           that the user defined
    Kwargs:
        page_no --- For pagination targets, this specifies which page we are on.
                    Certain vars will have the `pagination_increment` set
                    and we set them to `value + (page_no x pagination_increment)`
        product_listing_url --- Make this available to the user in their custom code
    Returns:
        A dictionary, with success key indicating if the build was a success or failure
        , an error key providing any error msgs and the next link url key providing the value
        of the next link URL built or None if it failed to be built
    '''
    # Init some defaults
    next_link = None
    error = None
    success = False
    nlf = None

    # User input function for construction of next link
    try:
        nlf = _build_next_link_func(paginationconf.build_next_link_func)
    except SyntaxError:
        logger.error('The function built from your code has invalid syntax')
        return {'success': False, 'error': 'The function built from your code has invalid syntax',
                'next_link': None}

    # Get the scraped variables
    var_dict = {}
    for var in paginationconf.scrapedvariables.all():
        var_dict[var.var_name] = u''  # Default
        var_value = var.var_value
        var_pagination_increment = var.pagination_increment
        var_multiple_vals = var.multiple_vals
        var_allow_absent = var.allow_absent
        # Uses the jsonpath, xpath, regex, line_number to extract a value for this var from resp
        xvarval = _extract_var_value(scrapy_resp, var.jsonpath, var.xpath,
                                     var.regex_pattern, var.line_number, var_multiple_vals, var_allow_absent,
                                     product_listing_url=product_listing_url)
        if var_value is not None and var_value is not u'':
            # User specifided static var value, use it as priority
            var_dict[var.var_name] = unicode(var_value)
        elif xvarval is not None:
            if not var_multiple_vals:
                var_dict[var.var_name] = unicode(xvarval)
            else:
                var_dict[var.var_name] = xvarval
        # If this is a pagination affected var, increment it accordingly
        if page_no > 0 and var_pagination_increment is not None and not var_multiple_vals:
            try:
                var_dict[var.var_name] = int(var_dict[var.var_name]) + (int(var_pagination_increment) * int(page_no))
                logger.debug('Incremented %s to: %s' % (var.var_name, var_dict[var.var_name]))
            except ValueError as verr:
                # couldn't convert var_dict[var.var_name] to int (user entered non
                # numeric value, or one was extracted, despite also having pg incr)
                error = 'Error doing pagination incremenet: \n%s' % str(verr)
                logger.error(error)
                return {'success': False, 'error': error, 'next_link': None}

    if nlf:
        # Now we use this dynamically gen function called ppf to gen xhr
        try:
            next_link = nlf(product_listing_url=product_listing_url, **var_dict)
            success = True
            logger.debug('Used custom nlf to build next link: %s' % next_link)
        except Exception as err:
            # Some exception with user code at runtime
            logger.error('Runtime error with user nlf function: %s' % str(err))
            error = 'The function built from your code had runtime error'
            success = False

    logger.debug('Returning next link:%s' % next_link)
    return {'success': success, 'next_link': next_link,
            'error': error}


def make_valid_url(url, reference_url=None):
    '''
    Check that a url has a netloc and a scheme, and
    if not use the referene_url provided to make the URL absolute.

    Args:
        url --- The URL to be considered
        reference_url --- The URL to be used as a reference to be build
                        and absolute URL of `url` if it's partial.
    Returns:
        A URL with complete scheme, netloc etc
    '''
    if url is None:
        return reference_url

    # Remove whitespace around url
    url = url.strip()

    # This is a hack for laithwaites because they have an unwanted substring
    # in all their source URLs of the form "&;jsessionid=SFSF534;"
    url = re.sub('\&\;jsessionid=.*\;', '-', url)

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
        # Python requests will percent encode only unreserved chars, e.g.
        # spaces or unicode chars (utf8 encodes then percent encodes). Other
        # characters like '%' itself which is often used in alcohol vendor URLs
        # remain unencoded, which can be problematic
        # o = o._replace(path=urllib.quote(o.path))
        # Use urlunparse to reconstruct
        return(urlunparse(o))
    except AttributeError:
        return url
