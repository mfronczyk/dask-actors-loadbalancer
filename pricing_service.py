"""
Coordinates pricing execution using Dask Actors.
"""

import asyncio
import logging
from random import shuffle
import sys
import time
from typing import Any

from actors import PricingActor
from load_balancer import LoadBalancer, ClusterClient
from risk import RiskRequest, RiskResponse
import dask.distributed

total_num_requests: int = 0


class DaskClient(ClusterClient):
    """ Load balancer's cluster client using Dask. """
    from dask.distributed import Client

    client: Client

    def __init__(self, client: Client):
        self.client = client

    async def create_actor(self) -> Any:
        actor = await self.client.submit(PricingActor, actor=True)
        await actor.start()
        return actor

    async def submit_request(self, request: RiskRequest, actor: Any) -> RiskResponse:
        return await actor.calculate_risk(request)


class RayClient(ClusterClient):
    """ Load balancer's cluster client using Ray. """

    async def create_actor(self) -> Any:
        actor = PricingActor.remote()
        await actor.start.remote()
        return actor

    async def submit_request(self, request: RiskRequest, actor: Any) -> RiskResponse:
        return await actor.calculate_risk.remote(request)


async def submit_request(batch_name: str, lb) -> RiskResponse:
    risk_request = RiskRequest(batch_name, "RATES_INFINITY_LONDON_123")
    response = await lb.submit(risk_request)
    global total_num_requests
    total_num_requests += 1
    return response


async def warmup_caches(lb) -> None:
    logging.info("Warming up the engines...")
    frt_requests = [submit_request("LONDONFLOW", lb) for _ in range(20)]
    tms_fx_requests = [submit_request("TMS FX", lb) for _ in range(20)]
    swaps_requests = [submit_request("SWAPS", lb) for _ in range(2)]
    other_requests = [submit_request("OTHER", lb) for _ in range(2)]
    await asyncio.gather(*(frt_requests + tms_fx_requests + swaps_requests + other_requests))
    logging.info("Warmup finished.")


async def upload_code_to_dask_cluster(client: dask.distributed.Client) -> None:
    """ Uploads our modules to the cluster. Without it Dask fails in the distributed mode. """
    # TODO: Find a better way to make workers discover our modules.
    await client.upload_file("pricing.py")
    await client.upload_file("risk.py")
    await client.upload_file("calculator.py")
    await client.upload_file("supervisor.py")
    await client.upload_file("actors.py")


async def run_tests(lb: LoadBalancer) -> None:
    """ Warmup caches on the calculators and then simulate lots of requests coming at the same time. """
    await lb.create_actors_for_heavy_requests()

    await warmup_caches(lb)

    global total_num_requests
    logging.info("Total number of warmup requests: %s", total_num_requests)

    total_num_requests = 0  # Start from scratch for real requests

    frt_requests = [submit_request("LONDONFLOW", lb) for _ in range(300)]
    tms_fx_requests = [submit_request("TMS FX", lb) for _ in range(300)]
    swaps_requests = [submit_request("SWAPS", lb) for _ in range(30)]
    other_requests = [submit_request("OTHER", lb) for _ in range(20)]

    requests = frt_requests + tms_fx_requests + swaps_requests + other_requests
    shuffle(requests)

    start = time.time()
    await asyncio.gather(*requests)
    end = time.time()

    logging.info("Total number of requests without warmup: %s, time %s",
                 total_num_requests, end - start)
    logging.info(lb)


async def main_dask() -> None:
    """ Run tests using Dask implementation. Requires removing Ray annotation from actors.py. """
    from dask.distributed import Client

    # async with Client("127.0.0.1: 8786", asynchronous=True) as client:
    async with Client(asynchronous=True) as client:
        logging.info(client)

        await upload_code_to_dask_cluster(client)

        lb = LoadBalancer(DaskClient(client))

        await run_tests(lb)


async def main_ray() -> None:
    """ Run tests using Ray implementation. Requires uncommenting Ray annotation from actors.py. """
    import ray

    ray.init()

    logging.info("Ray cluster nodes %s", len(ray.nodes()))

    lb = LoadBalancer(RayClient())

    await run_tests(lb)


if __name__ == '__main__':
    logging.basicConfig(format='%(asctime)s %(levelname)-8s SERVICE %(message)s',
                        stream=sys.stdout, level=logging.INFO)
    asyncio.run(main_dask())
    # asyncio.run(main_ray())
