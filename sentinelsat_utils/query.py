import os
import time
import logging
import datetime
from collections import OrderedDict

from tqdm import trange
from sentinelsat import SentinelAPIError
import dateutil.parser

logger = logging.getLogger(__name__)


def loop_query(api, query_kwargs, max_retries=5, timeout_minutes=5):
    """Loop api.query with a timeout on SentinelAPIError
    Parameters
    ----------
    api : sentinelsat.SentinelAPI
    query_kwargs : dict
        keyword arguments passed to api.query
    max_retries : n
        maximum number of retries
        set to 1 to disable retries
    timeout_minutes : n
        timeout between retries
        set to None to disable retries
    """
    err_reraise = None
    for _ in range(max_retries):
        try:
            return api.query(**query_kwargs)
        except SentinelAPIError as err:
            err_reraise = err
            if not timeout_minutes:
                break
            logger.info(
                'An exception occurred contacting DHUS (%s). Waiting %d minutes.',
                err, timeout_minutes)
            for t in trange(timeout_minutes * 6, desc='waiting'):
                time.sleep(10)
            continue
    if err_reraise is not None:
        raise err_reraise


def query_tiles_dates(api, tiles_dates, query_kwargs, loop_kwargs={}):
    """Query by tiles and dates
    Parameters
    ----------
    api : sentinelsat.SentinelAPI
        download API
    tiles_dates : dict
        mapping tilename (DDZZZ) -> list of datestr
    query_kwargs : dict
        keyword arguments passed to api.query
    Returns
    -------
    products : OrderedDict
        concatenated results
        from api.query
    """
    products = OrderedDict()
    for tile in tiles_dates:
        logger.info('-- tile %s', tile)
        for date in tiles_dates[tile]:
            kw = query_kwargs.copy()
            start_date = dateutil.parser.parse(date)
            end_date = start_date + datetime.timedelta(days=1)
            kw['date'] = (start_date, end_date)
            if start_date < datetime.datetime(2016, 11, 1):
                raise ValueError(
                    'Search by tile works only for single-tile (past 11/2016) format.')
            elif start_date < datetime.datetime(2017, 3, 31):
                kw['filename'] = '*_T{}_*'.format(tile)
            else:
                kw['tileid'] = tile
            pp = loop_query(api, query_kwargs=kw, **loop_kwargs)
            logger.info('   -- date %s (%d)', date, len(pp))
            products.update(pp)
    return products


def query_dates(api, dates, query_kwargs, loop_kwargs={}):
    """Query by dates
    Parameters
    ----------
    api : sentinelsat.SentinelAPI
        download API
    dates : list of str
        dates to search for
    query_kwargs : dict
        keyword arguments passed to api.query
    Returns
    -------
    products : OrderedDict
        concatenated results
        from api.query
    """
    products = OrderedDict()
    for date in dates:
        kw = query_kwargs.copy()
        start_date = dateutil.parser.parse(date)
        end_date = start_date + datetime.timedelta(days=1)
        kw['date'] = (start_date, end_date)
        pp = loop_query(api, query_kwargs=kw, **loop_kwargs)
        logger.info('   -- date %s (%d)', date, len(pp))
        products.update(pp)
    return products


def query_rel_orbit_numbers(api, rel_orbit_numbers, query_kwargs, loop_kwargs={}):
    """Query specific relative orbit numbers
    Parameters
    ----------
    api : sentinelsat.SentinelAPI
        download API
    rel_orbit_numbers : list of int
        relative orbit numbers to query
    query_kwargs : dict
        keyword arguments passed to api.query
    Returns
    -------
    products : OrderedDict
        concatenated results
        from api.query
    """
    products = OrderedDict()
    for ron in rel_orbit_numbers:
        kw = query_kwargs.copy()
        kw['relativeorbitnumber'] = str(ron)
        pp = loop_query(api, query_kwargs=kw, **loop_kwargs)
        products.update(pp)
    return products


def delete_empty(results):
    """Delete empty files"""
    for key in results:
        zipfile = results[key]['path']
        if os.path.getsize(zipfile) == 0:
            try:
                os.remove(zipfile)
            except OSError:
                logger.warn(
                    'Download produced empty file: \'%s\' that could not be deleted.',
                    zipfile)
