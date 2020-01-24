#!/usr/bin/env python3

from functools import wraps

from time import time, strftime, gmtime
from datetime import timedelta

from inpe_stac.log import logging


def log_function_header(function):

    @wraps(function)
    def wrapper(*args, **kwargs):
        logging.info('{0}() - execution'.format(function.__name__))

        return function(*args, **kwargs)

    return wrapper


def log_function_footer(function):

    @wraps(function)
    def wrapper(*args, **kwargs):
        start_time = time()

        result = function(*args, **kwargs)

        elapsed_time = time() - start_time

        logging.info('{0}() - elapsed time: {1}'.format(
            function.__name__,
            timedelta(seconds=elapsed_time)
        ))

        return result

    return wrapper
