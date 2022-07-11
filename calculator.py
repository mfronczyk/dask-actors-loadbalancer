"""
Calculator which needs to run in Python 2.7.
"""

import json
import logging
import pickle
import random
import sys
from time import sleep
from pricing import SEPARATOR, PricingResponse


def version():
    """ Return python version. """
    return '.'.join('%d' % e for e in sys.version_info[0:3])


num_calls = 0


def calculate():
    """ Run calculations. Sleeps long on the first run and then 1s on subsequent calls to simulate warming up. """
    global num_calls
    if num_calls == 0:
        sleep(20)
    else:
        sleep(random.uniform(0.5, 1.5))
    num_calls += 1
    logging.info("Calculation %s finished.", num_calls)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)-8s CALCULATOR %(message)s',
                        stream=sys.stderr, level=logging.INFO)
    logging.info("Calculator started. Python version = %s", version())
    while True:
        logging.debug("Waiting for request...")
        request = pickle.load(sys.stdin)
        logging.info("Request received: %s", json.dumps(request.__dict__))
        calculate()
        response = PricingResponse(request, "OK")
        logging.debug("Sending response...")
        pickle.dump(response, sys.stdout, protocol=2)
        sys.stdout.write(SEPARATOR)
        sys.stdout.flush()
        logging.info("Response sent.")
