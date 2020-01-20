#!/usr/bin/env python3

import os
import json

from datetime import datetime
from collections import OrderedDict
from copy import deepcopy

import sqlalchemy
from sqlalchemy.sql import text

from pprint import pprint
import logging

handler = logging.FileHandler('inpe_stac.log')
handler.setFormatter(logging.Formatter(
    '[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
))

logger = logging.getLogger('data')
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


def get_collection_items(collection_id=None, item_id=None, bbox=None, time=None, type=None, ids=None, bands=None,
                         collections=None, page=1, limit=10):

    params = deepcopy(locals())
    params['page'] = (page - 1) * limit

    logging.warning('get_collection_items - params - {}'.format(params))

    # sql = '''SELECT a.*, b.Dataset
    #          FROM Scene  a, Product  b, Dataset  c, Qlook d
    #          WHERE '''
    sql = '''SELECT a.*, b.Dataset FROM Scene  a, Product  b, Dataset  c WHERE '''

    where = list()

    where.append('a.SceneId = b.SceneId')

    if ids is not None:
        where.append("FIND_IN_SET(b.SceneId, :ids)")
    elif item_id is not None:
        where.append("b.SceneId = :item_id")
    else:
        if collections is not None:
            where.append("FIND_IN_SET(b.Dataset, :collections)")
        elif collection_id is not None:
            where.append("b.Dataset = :collection_id")
        if bbox is not None:
            try:
                for x in bbox.split(','):
                    float(x)
                params['min_x'], params['min_y'], params['max_x'], params['max_y'] = bbox.split(',')
                bbox = ""
                bbox += "((:min_x <= `TR_Longitude` and :min_y <=`TR_Latitude`)"
                bbox += " or "
                bbox +=  "(:min_x <= `BR_Longitude` and :min_y <=`TL_Latitude`))"
                bbox += " and "
                bbox += "((:max_x >= `BL_Longitude` and :max_y=`BL_Latitude`)"
                bbox += " or "
                bbox +=  "(:max_x >= `TL_Longitude` and :max_y >=`BR_Latitude`))"

                where.append("(" + bbox + ")")

            except:
                raise (InvalidBoundingBoxError())

            if time is not None:

                if "/" in time:
                    params['time_start'], end = time.split("/")
                    params['time_end'] = datetime.fromisoformat(end)
                    where.append("a.Date <= :time_end")
                else:
                    params['time_start'] = datetime.fromisoformat(time)
                where.append("a.Date >= :time_start")

    where = " and ".join(where)

    sql += where

    sql += " GROUP BY a.SceneId ORDER BY a.Date DESC"
    sql += " LIMIT :page, :limit"

    result = do_query(sql, **params)

    if result is not None:
        logging.warning('get_collection_items - {} - sql - {}'.format(len(result),sql))
    else:
        logging.warning('get_collection_items - no result - sql - {}'.format(sql))

    return result


def get_collections(collection_id=None):
    logging.warning('\n\nget_collections()')

    logging.warning('collection_id: {}'.format(collection_id))

    kwargs = {}
    where = ''

    # if there is a 'collection_id' key to search, then add the WHERE clause and the key to kwargs
    if collection_id is not None:
        where = 'WHERE id = :collection_id;'
        kwargs = { 'collection_id': collection_id }

    query = 'SELECT * FROM collection {};'.format(where)

    logging.warning('query: {}'.format(query))

    result = do_query(query, **kwargs)

    logging.warning('result: {}'.format(result))

    if result is not None:
        logging.warning('len(result): {} - query: {}'.format(len(result), query))
    else:
        logging.warning('no result - query: {}'.format(query))

    return result


def get_collection(collection_id):
    logging.warning('\n\nget_collection(collection_id)')

    result = get_collections(collection_id=collection_id)[0]

    collection = {}
    collection['id'] = collection_id
    start = result['start_date'].isoformat()
    end = None if result['end_date'] is None else result['end_date'].isoformat()

    collection["stac_version"] = os.getenv("API_VERSION")
    collection["description"] = result["description"]

    collection["license"] = None
    collection["properties"] = {}
    collection["extent"] = {"spatial": [result['min_x'],result['min_y'],result['max_x'],result['max_y']], "time": [start, end]}
    collection["properties"] = OrderedDict()

    return collection


def make_geojson(items, links):
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
        # pprint('\n\nSceneId: ', i['SceneId'])
        # pprint('item: ', i, '\n\n')

        feature = OrderedDict()

        feature['type'] = 'Feature'
        feature['id'] = i['SceneId']
        feature['collection'] = i['Dataset']

        geometry = dict()
        geometry['type'] = 'Polygon'
        geometry['coordinates'] = [
          [[i['TL_Longitude'], i['TL_Latitude']],
           [i['BL_Longitude'], i['BL_Latitude']],
           [i['BR_Longitude'], i['BR_Latitude']],
           [i['TR_Longitude'], i['TR_Latitude']],
           [i['TL_Longitude'], i['TL_Latitude']]]
        ]
        feature['geometry'] = geometry
        feature['bbox'] = bbox(feature['geometry']['coordinates'])

        feature['properties'] = {}
        feature['properties']['datetime'] = datetime.fromisoformat(str(i['Date'])).isoformat()

        sql = '''SELECT band, filename
             FROM Product WHERE SceneId = :item_id
             GROUP BY band, SceneId;
             '''
        feature['assets'] = {}

        assets = do_query(sql, item_id=i['SceneId'])
        for asset in assets:
            feature['assets'][asset['band']] = {'href': os.getenv('TIF_ROOT') + asset['filename']}

        feature['assets']['thumbnail'] = {'href': get_browse_image(i['SceneId'])}
        feature['links'] = deepcopy(links)
        feature['links'][0]['href'] += i['Dataset'] + "/items/" + i['SceneId']
        feature['links'][1]['href'] += i['Dataset']
        feature['links'][2]['href'] += i['Dataset']

        features.append(feature)

    if len(features) == 1:
        return features[0]

    gjson['features'] = features

    return gjson


# def make_geojson(data, totalResults, searchParams, output='json'):
#     geojson = dict()
#     geojson['totalResults'] = totalResults
#     geojson['type'] = 'FeatureCollection'
#     geojson['features'] = []
#     base_url = os.environ.get('BASE_URL')
#     for i in data:
#         feature = dict()
#         feature['type'] = 'Feature'

#         geometry = dict()
#         geometry['type'] = 'Polygon'
#         geometry['coordinates'] = [
#           [[i['TL_Longitude'], i['TL_Latitude']],
#            [i['BL_Longitude'], i['BL_Latitude']],
#            [i['BR_Longitude'], i['BR_Latitude']],
#            [i['TR_Longitude'], i['TR_Latitude']],
#            [i['TL_Longitude'], i['TL_Latitude']]]
#         ]

#         feature['geometry'] = geometry
#         properties = dict()
#         properties['title'] = i['SceneId']
#         properties['id'] = '{}/granule.{}?uid={}'.format(base_url, output, i['SceneId'])
#         properties['updated'] = i['IngestDate']
#         properties['alternate'] = '{}/granule.{}?uid={}'.format(base_url, output, i['SceneId'])
#         properties['icon'] = get_browse_image(i['SceneId'])
#         properties['via'] = '{}/metadata/{}'.format(base_url, i['SceneId'])

#         for key, value in i.items():
#             if key != 'SceneId' and key != 'IngestDate':
#                 properties[key.lower()] = value

#         products = get_products(i['SceneId'], searchParams)

#         properties['enclosure'] = []
#         for p in products:
#             enclosure = dict()

#             enclosure['band'] = p['Band']
#             enclosure['radiometric_processing'] = p['RadiometricProcessing']
#             enclosure['type'] = p['Type']
#             enclosure['url'] = os.environ.get('ENCLOSURE_BASE') + p['Filename']
#             properties['enclosure'].append(enclosure)

#         feature['properties'] = properties
#         geojson['features'].append(feature)

#     return geojson


def get_browse_image(sceneid):
    table = ''

    sql = "SELECT QLfilename FROM Qlook WHERE SceneId = :sceneid"

    result = do_query(sql, sceneid=sceneid)

    if result is not None:
        logging.warning('get_browse_image url - {}'.format(os.getenv('PNG_ROOT') + result[0]['QLfilename']))
        return os.getenv('PNG_ROOT') + result[0]['QLfilename']
    else:
        return None


def do_query(sql, **kwargs):
    connection = 'mysql://{}:{}@{}/{}'.format(os.environ.get('DB_USER'),
                                              os.environ.get('DB_PASS'),
                                              os.environ.get('DB_HOST'),
                                              os.environ.get('DB_NAME'))
    engine = sqlalchemy.create_engine(connection)

    sql = text(sql)
    engine.execute("SET @@group_concat_max_len = 1000000;")

    result = engine.execute(sql, kwargs)
    result = result.fetchall()

    engine.dispose()

    result = [dict(row) for row in result]

    if len(result) > 0:
        return result
    else:
        return None


def bbox(coord_list):
    box = []
    for i in (0, 1):
        res = sorted(coord_list[0], key=lambda x: x[i])
        box.append((res[0][i], res[-1][i]))
    ret = [box[0][0], box[1][0], box[0][1], box[1][1]]
    return ret


class InvalidBoundingBoxError(Exception):
    pass
