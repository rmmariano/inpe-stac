#!/usr/bin/env python3

"""
STAC API Specification

Specification: https://github.com/radiantearth/stac-spec/blob/master/api-spec/api-spec.md#stac-api-specification
OpenAPI definition: https://stacspec.org/STAC-ext-api.html
"""

from time import time as time_time, strftime, gmtime
from datetime import timedelta

from flask import Flask, jsonify, request, abort
from flasgger import Swagger

from inpe_stac import data
from inpe_stac.environment import BASE_URI, API_VERSION
from inpe_stac.log import logging


app = Flask(__name__)

app.config["JSON_SORT_KEYS"] = False
app.config["SWAGGER"] = {
    "openapi": "3.0.1",
    "specs_route": "/docs",
    "title": "INPE STAC Catalog"
}

swagger = Swagger(app, template_file="./spec/api/v0.7/STAC.yaml")


@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


##################################################
# OGC API - Features Endpoints
# Specification: https://github.com/radiantearth/stac-spec/blob/master/api-spec/api-spec.md#ogc-api---features-endpoints
##################################################

@app.route("/", methods=["GET"])
def index():
    links = [{"href": f"{BASE_URI}", "rel": "self"},
             {"href": f"{BASE_URI}docs", "rel": "service"},
             {"href": f"{BASE_URI}conformance", "rel": "conformance"},
             {"href": f"{BASE_URI}collections", "rel": "data"},
             {"href": f"{BASE_URI}stac", "rel": "data"},
             {"href": f"{BASE_URI}stac/search", "rel": "search"}]
    return jsonify(links)


@app.route("/conformance", methods=["GET"])
def conformance():
    conforms = {
        "conformsTo": [
            "http://www.opengis.net/spec/wfs-1/3.0/req/core",
            "http://www.opengis.net/spec/wfs-1/3.0/req/oas30",
            "http://www.opengis.net/spec/wfs-1/3.0/req/html",
            "http://www.opengis.net/spec/wfs-1/3.0/req/geojson"
        ]
    }

    return jsonify(conforms)


@app.route("/collections", methods=["GET"])
def collections():
    """
    Specification: https://github.com/radiantearth/stac-spec/blob/v0.7.0/collection-spec/collection-spec.md#collection-fields
    """

    result = data.get_collections()

    collections = {
        'collections': []
    }

    for collection in result:
        collections['collections'].append(
            {
                'stac_version': API_VERSION,
                'id': collection['id'],
                'title': collection['id'],
                'description': collection['description'],
                'license': '',
                'extent': [],
                'links': [
                    {
                        "href": f"{BASE_URI}collections/{collection['id']}",
                        "rel": "self"
                    },
                    {
                        "href": f"{BASE_URI}stac/",
                        "rel": "root"
                    }
                ]
            }
        )

    return jsonify(collections)


@app.route("/collections/<collection_id>", methods=["GET"])
def collections_id(collection_id):
    """
    Specification: https://github.com/radiantearth/stac-spec/blob/v0.7.0/collection-spec/collection-spec.md#collection-fields
    """

    result = data.get_collections(collection_id)[0]

    collection_id = result['id']

    start_date = result['start_date'].isoformat()
    end_date = None if result['end_date'] is None else result['end_date'].isoformat()

    collection = {
        'stac_version': API_VERSION,
        'id': collection_id,
        'title': collection_id,
        'description': result['description'],
        'license': None,
        'properties': {},
        'extent': {
            'spatial': [
                result['min_x'], result['min_y'], result['max_x'], result['max_y']
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

    return jsonify(collection)


@app.route("/collections/<collection_id>/items", methods=["GET"])
def collection_items(collection_id):
    """
    Example of full route:
        - http://localhost:8089/inpe-stac/collections/CB4A_MUX_L2_DN/items?bbox=-68.0273437,-25.0059726,-34.9365234,0.3515602&limit=10000&time=2019-12-22T00:00:00/2020-01-22T23:59:00
    """

    start_time = time_time()

    items = data.get_collection_items(collection_id=collection_id, bbox=request.args.get('bbox', None),
                                      time=request.args.get('time', None), type=request.args.get('type', None),
                                      page=int(request.args.get('page', 1)),
                                      limit=int(request.args.get('limit', 10)))

    links = [{"href": f"{BASE_URI}collections/", "rel": "self"},
             {"href": f"{BASE_URI}collections/", "rel": "parent"},
             {"href": f"{BASE_URI}collections/", "rel": "collection"},
             {"href": f"{BASE_URI}stac", "rel": "root"}]

    gjson = data.make_geojson(items, links)

    elapsed_time = time_time() - start_time

    logging.info('/collections/<collection_id>/items - elapsed time: {}'.format(timedelta(seconds=elapsed_time)))

    return jsonify(gjson)


@app.route("/collections/<collection_id>/items/<item_id>", methods=["GET"])
def items_id(collection_id, item_id):
    item = data.get_collection_items(collection_id=collection_id, item_id=item_id)

    links = [{"href": f"{BASE_URI}collections/", "rel": "self"},
             {"href": f"{BASE_URI}collections/", "rel": "parent"},
             {"href": f"{BASE_URI}collections/", "rel": "collection"},
             {"href": f"{BASE_URI}stac", "rel": "root"}]

    gjson = data.make_geojson(item, links)

    return jsonify(gjson)


##################################################
# STAC Endpoints
# Specification: https://github.com/radiantearth/stac-spec/blob/master/api-spec/api-spec.md#stac-endpoints
##################################################

@app.route("/stac", methods=["GET"])
def stac():
    """
    Specification: https://github.com/radiantearth/stac-spec/blob/v0.7.0/catalog-spec/catalog-spec.md#catalog-fields
    """

    collections = data.get_collections()

    catalog = {
        "stac_version": API_VERSION,
        "id": "inpe-stac",
        "description": "INPE STAC Catalog",
        "links": [
            {
                "href": f"{BASE_URI}stac",
                "rel": "self"
            }
        ]
    }

    for collection in collections:
        catalog["links"].append(
            {
                "href": f"{BASE_URI}collections/{collection['id']}",
                "rel": "child",
                "title": collection['id']
            }
        )

    return jsonify(catalog)


@app.route("/stac/search", methods=["GET", "POST"])
def stac_search():
    logging.info('\n\nstac_search()')

    start_time = time_time()

    bbox, time, ids, collections, page, limit = None, None, None, None, None, None
    if request.method == "POST":
        if request.is_json:
            request_json = request.get_json()

            bbox = request_json.get('bbox', None)
            if bbox is not None:
                bbox = ",".join([str(x) for x in bbox])

            time = request_json.get('time', None)

            ids = request_json.get('ids', None)
            if ids is not None:
                ids = ",".join([x for x in ids])

            collections = request_json.get('collections', None)

            page = int(request_json.get('page', 1))
            limit = int(request_json.get('limit', 10))
        else:
            abort(400, "POST Request must be an application/json")

    elif request.method == "GET":
        bbox = request.args.get('bbox', None)
        time = request.args.get('time', None)
        ids = request.args.get('ids', None)
        collections = request.args.get('collections', None)
        page = int(request.args.get('page', 1))
        limit = int(request.args.get('limit', 10))

    items = data.get_collection_items(collections=collections, bbox=bbox, time=time,
                                      ids=ids, page=page, limit=limit)

    links = [{"href": f"{BASE_URI}collections/", "rel": "self"},
             {"href": f"{BASE_URI}collections/", "rel": "parent"},
             {"href": f"{BASE_URI}collections/", "rel": "collection"},
             {"href": f"{BASE_URI}stac", "rel": "root"}]

    gjson = data.make_geojson(items, links=links)

    gjson['meta'] = {
        'page': page,
        'limit': limit,
        'returned': len(gjson['features'])
    }

    elapsed_time = time_time() - start_time

    logging.info('/stac/search - elapsed time: {}'.format(timedelta(seconds=elapsed_time)))

    return jsonify(gjson)


##################################################
# Error Endpoints
##################################################

@app.errorhandler(400)
def handle_bad_request(e):
    resp = jsonify({'code': '400', 'description': 'Bad Request - {}'.format(e.description)})
    resp.status_code = 400

    return resp


@app.errorhandler(404)
def handle_page_not_found(e):
    resp = jsonify({'code': '404', 'description': 'Page not found'})
    resp.status_code = 404

    return resp


@app.errorhandler(500)
def handle_api_error(e):
    resp = jsonify({'code': '500', 'description': 'Internal Server Error'})
    resp.status_code = 500

    return resp


@app.errorhandler(502)
def handle_bad_gateway_error(e):
    resp = jsonify({'code': '502', 'description': 'Bad Gateway'})
    resp.status_code = 502

    return resp


@app.errorhandler(503)
def handle_service_unavailable_error(e):
    resp = jsonify({'code': '503', 'description': 'Service Unavailable'})
    resp.status_code = 503

    return resp


@app.errorhandler(Exception)
def handle_exception(e):
    app.logger.exception(e)
    resp = jsonify({'code': '500', 'description': 'Internal Server Error'})
    resp.status_code = 500

    return resp


##################################################
# Main
##################################################

if __name__ == '__main__':
    app.run()
