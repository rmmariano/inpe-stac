#!/usr/bin/env python3

from os import getenv
from json import loads
from pprint import PrettyPrinter
from werkzeug.exceptions import BadRequest

from datetime import datetime
from copy import deepcopy
from collections import OrderedDict

import sqlalchemy
from sqlalchemy.sql import text

from inpe_stac.log import logging
from inpe_stac.decorator import log_function_header
from inpe_stac.environment import BASE_URI, API_VERSION


pp = PrettyPrinter(indent=4)


def len_result(result):
    return len(result) if result is not None else len([])


@log_function_header
def get_collections(collection_id=None):
    logging.info('get_collections - collection_id: {}'.format(collection_id))

    kwargs = {}
    where = ''

    # if there is a 'collection_id' key to search, then add the WHERE clause and the key to kwargs
    if collection_id is not None:
        where = 'WHERE id = :collection_id'
        kwargs = { 'collection_id': collection_id }

    query = 'SELECT * FROM stac_collection {};'.format(where)

    logging.info('get_collections - query: {}'.format(query))

    result = do_query(query, **kwargs)

    logging.info('get_collections - len(result): {}'.format(len_result(result)))
    # logging.debug('get_collections - result: {}'.format(result))

    return result


@log_function_header
def __search_stac_item_view(where, params):
    logging.info('__search_stac_item_view()\n')

    sql = '\nSELECT * FROM stac_item \nWHERE '
    sql_count = '\nSELECT COUNT(id) as matched FROM stac_item \nWHERE '

    # create where and limit clauses
    where = '\nAND '.join(where)
    limit = '\nLIMIT :page, :limit'

    # logging.info('__search_stac_item_view() - where: {}'.format(where))
    logging.info('__search_stac_item_view() - params: {}'.format(params))

    # add just where clause to query, because I want to get the number of total results
    sql_count += where
    # add where and limit clauses to query
    sql += where + limit

    # logging.info('__search_stac_item_view() - sql_count: {}'.format(sql_count))
    logging.info('__search_stac_item_view() - sql: {}'.format(sql))

    # execute the queries
    result_count = do_query(sql_count, **params)
    result = do_query(sql, **params)

    matched = result_count[0]['matched']

    logging.info('__search_stac_item_view() - returned: {}'.format(len_result(result)))
    logging.info('__search_stac_item_view() - matched: {}'.format(matched))
    # logging.debug('__search_stac_item_view() - result: \n\n{}\n\n'.format(result))

    return result, matched


@log_function_header
def get_collection_items(collection_id=None, item_id=None, bbox=None, time=None,
                         intersects=None, page=1, limit=10, ids=None, collections=None,
                         query=None):
    logging.info('get_collection_items()')

    result = []
    metadata_related_to_collections = []
    matched = 0

    params = {
        'page': page - 1,
        'limit': limit
    }

    default_where = []

    # search for ids
    if item_id is not None or ids is not None:
        if item_id is not None:
            default_where.append('id = :item_id')
            params['item_id'] = item_id
        elif ids is not None:
            default_where.append('FIND_IN_SET(id, :ids)')
            params['ids'] = ids

        logging.info('get_collection_items() - default_where: {}'.format(default_where))

        __result, __matched = __search_stac_item_view(default_where, params)

        result += __result
        matched += __matched

    else:
        if bbox is not None:
            try:
                for x in bbox.split(','):
                    float(x)

                params['min_x'], params['min_y'], params['max_x'], params['max_y'] = bbox.split(',')

                # replace method removes extra espace caused by multi-line String
                default_where.append(
                    '''(
                    ((:min_x <= tr_longitude and :min_y <= tr_latitude)
                    or
                    (:min_x <= br_longitude and :min_y <= tl_latitude))
                    and
                    ((:max_x >= bl_longitude and :max_y >= bl_latitude)
                    or
                    (:max_x >= tl_longitude and :max_y >= br_latitude))
                    )'''.replace('                ', '')
                )
            except:
                raise (InvalidBoundingBoxError())

        if time is not None:
            if not (isinstance(time, str) or isinstance(time, list)):
                raise BadRequest('`time` field is not a string or list')

            # if time is a string, then I convert it to list by splitting it
            if isinstance(time, str):
                time = time.split("/")

            # if there is time_start and time_end, then get them
            if len(time) == 2:
                params['time_start'], params['time_end'] = time
                default_where.append("date <= :time_end")
            # if there is just time_start, then get it
            elif len(time) == 1:
                params['time_start'] = time[0]

            default_where.append("date >= :time_start")

        logging.info('get_collection_items() - default_where: {}'.format(default_where))

        # if query is a dict, then get all available fields to search
        # Specification: https://github.com/radiantearth/stac-spec/blob/v0.7.0/api-spec/extensions/query/README.md
        if isinstance(query, dict):
            for field, value in query.items():
                # eq, neq, lt, lte, gt, gte
                if 'eq' in value:
                    default_where.append('{0} = {1}'.format(field, value['eq']))
                if 'neq' in value:
                    default_where.append('{0} != {1}'.format(field, value['neq']))
                if 'lt' in value:
                    default_where.append('{0} < {1}'.format(field, value['lt']))
                if 'lte' in value:
                    default_where.append('{0} <= {1}'.format(field, value['lte']))
                if 'gt' in value:
                    default_where.append('{0} > {1}'.format(field, value['gt']))
                if 'gte' in value:
                    default_where.append('{0} >= {1}'.format(field, value['gte']))
                # startsWith, endsWith, contains
                if 'startsWith' in value:
                    default_where.append('{0} LIKE \'{1}%\''.format(field, value['startsWith']))
                if 'endsWith' in value:
                    default_where.append('{0} LIKE \'%{1}\''.format(field, value['endsWith']))
                if 'contains' in value:
                    default_where.append('{0} LIKE \'%{1}%\''.format(field, value['contains']))

        if collection_id is not None and isinstance(collection_id, str):
            collections = [collection_id]

        # search for collections
        if collections is not None:
            logging.info('get_collection_items() - collections: {}'.format(collections))

            for collection_id in collections:
                logging.info('get_collection_items() -     collection_id: {}'.format(collection_id))

                where = ['collection = :collection_id']
                params['collection_id'] = collection_id

                where += default_where

                __result, __matched = __search_stac_item_view(where, params)

                result += __result
                matched += __matched

                metadata_related_to_collections.append(
                    {
                        'name': collection_id,
                        'context': {
                            'page': params['page'],
                            'limit': params['limit'],
                            'matched': __matched,
                            'returned': len(__result)
                        }
                    }
                )

        # search for anything else
        else:
            __result, __matched = __search_stac_item_view(default_where, params)

            result += __result
            matched += __matched

    logging.info('get_collection_items() - matched: {}'.format(matched))
    # logging.debug('get_collection_items() - result: \n\n{}\n\n'.format(result))
    logging.debug('get_collection_items() - \n\nmetadata_related_to_collections: {}\n\n'.format(metadata_related_to_collections))

    return result, matched, metadata_related_to_collections


def make_json_collection(collection_result):
    collection_id = collection_result['id']

    start_date = collection_result['start_date'].isoformat()
    end_date = None if collection_result['end_date'] is None else collection_result['end_date'].isoformat()

    collection = {
        'stac_version': API_VERSION,
        'id': collection_id,
        'title': collection_id,
        'description': collection_result['description'],
        'license': None,
        'properties': {},
        'extent': {
            'spatial': [
                collection_result['min_x'], collection_result['min_y'],
                collection_result['max_x'], collection_result['max_y']
            ],
            'time': [ start_date, end_date ]
        },
        'links': [
            {'href': f'{BASE_URI}collections/{collection_id}', 'rel': 'self'},
            {'href': f'{BASE_URI}collections/{collection_id}/items', 'rel': 'items'},
            {'href': f'{BASE_URI}collections', 'rel': 'parent'},
            {'href': f'{BASE_URI}collections', 'rel': 'root'},
            {'href': f'{BASE_URI}stac', 'rel': 'root'}
        ]
    }

    return collection


def make_json_items(items, links):
    # logging.debug('make_geojson - items: {}'.format(items))
    # logging.debug('make_geojson - links: {}'.format(links))

    if items is None:
        return {
            'type': 'FeatureCollection',
            'features': []
        }

    features = []

    gjson = OrderedDict()
    gjson['type'] = 'FeatureCollection'

    if len(items) == 0:
        gjson['features'] = features
        return gjson

    for i in items:
        # print('\n\nid: ', i['id'])
        # print('item: ', end='')
        # pp.pprint(i)
        # print('\n\n')

        # datetime should be the value of i['center_time'], but if its value is None,
        # then get the value of i['date']
        _datetime = i['center_time'] if i['center_time'] is not None else i['date']
        # format the datetime
        _datetime = datetime.fromisoformat(str(_datetime)).isoformat()

        feature = OrderedDict()

        feature['type'] = 'Feature'
        feature['id'] = i['id']
        feature['collection'] = i['collection']

        geometry = dict()
        geometry['type'] = 'Polygon'
        geometry['coordinates'] = [
          [[i['tl_longitude'], i['tl_latitude']],
           [i['bl_longitude'], i['bl_latitude']],
           [i['br_longitude'], i['br_latitude']],
           [i['tr_longitude'], i['tr_latitude']],
           [i['tl_longitude'], i['tl_latitude']]]
        ]
        feature['geometry'] = geometry
        feature['bbox'] = bbox(feature['geometry']['coordinates'])

        feature['properties'] = {
            'datetime': _datetime,
            'path': i['path'],
            'row': i['row'],
            'satellite': i['satellite'],
            'sensor': i['sensor'],
            'cloud_cover': i['cloud_cover'],
            'sync_loss': i['sync_loss']
        }

        feature['assets'] = {}

        # convert string json to dict json
        i['assets'] = loads(i['assets'])

        for asset in i['assets']:
            feature['assets'][asset['band']] = {'href': getenv('TIF_ROOT') + asset['href']}

        feature['assets']['thumbnail'] = {'href': getenv('PNG_ROOT') + i['thumbnail']}

        feature['links'] = deepcopy(links)
        feature['links'][0]['href'] += i['collection'] + "/items/" + i['id']
        feature['links'][1]['href'] += i['collection']
        feature['links'][2]['href'] += i['collection']

        features.append(feature)

        # print('\nfeature: ')
        # pp.pprint(feature)
        # print('\n')

    gjson['features'] = features

    # logging.debug('make_geojson - gjson: {}'.format(gjson))

    return gjson


def do_query(sql, **kwargs):
    connection = 'mysql://{}:{}@{}/{}'.format(
        getenv('DB_USER'), getenv('DB_PASS'), getenv('DB_HOST'), getenv('DB_NAME')
    )
    engine = sqlalchemy.create_engine(connection)

    sql = text(sql)
    engine.execute("SET @@group_concat_max_len = 1000000;")

    result = engine.execute(sql, kwargs)
    result = result.fetchall()

    engine.dispose()

    result = [ dict(row) for row in result ]

    if len(result) > 0:
        return result
    else:
        return None


def bbox(coord_list):
    box = []

    for i in (0, 1):
        res = sorted(coord_list[0], key=lambda x: x[i])
        box.append((res[0][i], res[-1][i]))

    return [box[0][0], box[1][0], box[0][1], box[1][1]]


class InvalidBoundingBoxError(Exception):
    pass
