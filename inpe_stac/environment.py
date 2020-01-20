#!/usr/bin/env python3

from os import getenv
from logging import DEBUG, INFO


BASE_URI = getenv('BASE_URI', 'http://cdsr.dpi.inpe.br/inpe-stac/')

FLASK_ENV = getenv('FLASK_ENV', 'production')

# default logging level in production server
LOGGING_LEVEL = INFO

# if the application is in development mode, then change the logging level and debug mode
if FLASK_ENV == 'development':
    LOGGING_LEVEL = DEBUG

API_VERSION = getenv('API_VERSION', '0.7')
