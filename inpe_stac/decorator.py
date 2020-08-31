
from functools import wraps
from time import time, strftime, gmtime
from datetime import timedelta
from traceback import format_exc, print_stack
from werkzeug.exceptions import InternalServerError

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


def catch_generic_exceptions(function):

    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            # try to execute the function
            return function(*args, **kwargs)

        # generic exception
        except Exception as error:
            error_message = 'An unexpected error ocurred. Please, contact the administrator.' + '\nError: ' + str(error)

            print_traceback = 'Error message: {0}\n{1}'.format(error_message, format_exc())

            logging.info('{0}() - {1}'.format(
                function.__name__,
                print_traceback
            ))

            raise InternalServerError(error_message + 'Error: ' + str(error))

    return wrapper
