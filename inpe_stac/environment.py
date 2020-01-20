#!/usr/bin/env python3

from os import environ
from logging import DEBUG, INFO

os_environ_get = environ.get


FLASK_ENV = os_environ_get('FLASK_ENV', 'production')

SERVER_HOST = os_environ_get('SERVER_HOST', '0.0.0.0')

try:
    SERVER_PORT = int(os_environ_get('SERVER_PORT', '5000'))
except ValueError:
    SERVER_PORT = 5000

# default logging level in production server
LOGGING_LEVEL = INFO
# default debug mode in production server
DEBUG_MODE = False

# if the application is in development mode, then change the logging level and debug mode
if FLASK_ENV == 'development':
    LOGGING_LEVEL = DEBUG
    DEBUG_MODE = True
