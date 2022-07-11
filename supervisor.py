"""
Manages the child Python 2.7 process.
"""

import asyncio
from asyncio.subprocess import Process
import json
import logging
import pickle
from typing import Optional
from pricing import SEPARATOR, PricingRequest, PricingResponse


class Supervisor:
    """Manages the child calculator."""

    process: Optional[Process]
    lock: asyncio.Lock
    num_calcs: int
    cached_request_name: Optional[str]

    def __init__(self) -> None:
        self.process = None
        self.lock = asyncio.Lock()
        self.num_calcs = 0
        self.cached_request_name = None

    def __del__(self) -> None:
        """ Stops the calculator process before the object gets deleted. """
        self.stop()

    async def ensure_start(self) -> None:
        """ Start worker process. """

        async with self.lock:
            if self.process:
                return

            # TODO: There should be better way than resetting the root logger every time a worker is started.
            logging.basicConfig(
                format='%(asctime)s %(levelname)-8s SUPERVISOR %(message)s', level=logging.INFO)

            logging.info("Starting worker...")
            self.process = await asyncio.create_subprocess_exec('/Users/michal/Projects/dask/.python2.7/bin/python', 'calculator.py',
                                                                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
            await asyncio.sleep(5)  # Simulate slow to start process
            logging.info("Worker started")

    def stop(self) -> None:
        """ Stop the worker process."""
        if not self.process:
            return
        logging.debug("Stopping worker...")
        self.process.terminate()
        logging.debug(
            "Worker stopped. Cached request %s. Number of calculations was %s.", self.cached_request_name, self.num_calcs)

    async def __send_request(self, request: PricingRequest) -> None:
        logging.info("Sending request: %s", json.dumps(request.__dict__))
        pickled_request: bytes = pickle.dumps(request, protocol=2)
        self.process.stdin.write(pickled_request)
        await self.process.stdin.drain()

    async def __read_response(self) -> PricingResponse:
        logging.debug("Waiting for response...")
        pickled_response: bytes = await self.process.stdout.readuntil(SEPARATOR.encode())
        response: PricingResponse = pickle.loads(pickled_response)
        logging.info("Response: %s", response.status)
        logging.debug("Request: %s", json.dumps(response.request.__dict__))
        return response

    def __check_if_request_matches_cache(self, request: PricingRequest) -> None:
        if not self.cached_request_name:
            self.cached_request_name = request.name
            logging.info("Cached request %s", self.cached_request_name)
        elif not self.cached_request_name == request.name:
            logging.warning("Received request %s, but the cached one is %s",
                            request.name, self.cached_request_name)

    async def calculate(self, request: PricingRequest) -> PricingResponse:
        """ Send pricing request to the child process."""
        async with self.lock:
            self.__check_if_request_matches_cache(request)
            await self.__send_request(request)
            response = await self.__read_response()
            self.num_calcs += 1
            return response


async def main() -> None:
    supervisor = Supervisor()
    await supervisor.ensure_start()
    for _ in range(10):
        await supervisor.calculate(PricingRequest("Test request"))
    supervisor.stop()


if __name__ == '__main__':
    asyncio.run(main())
