"""
Module for efficient load balancing of the risk requests among the available calculators.
"""


from abc import abstractmethod
import asyncio
import logging
from typing import Any, Protocol
from risk import RiskRequest, RiskResponse


class ClusterClient(Protocol):
    @abstractmethod
    async def create_actor(self) -> Any:
        """ Creates an actor and returns some handle to it that can be later passed to submit(). """
        raise NotImplementedError

    @abstractmethod
    async def submit_request(self, request: RiskRequest, actor: Any) -> RiskResponse:
        """ Submits risk request to the provided actor previously created using create_actor(). """
        raise NotImplementedError


class ActorsList:
    """ Holds the list of actors currently waiting for new tasks. """
    actors: list[Any]
    condition: asyncio.Condition

    def __init__(self, actors=None) -> None:
        if actors:
            self.actors = actors
        else:
            self.actors = []
        self.condition = asyncio.Condition()

    async def append(self, actor: Any) -> None:
        """ Adds actor to the end of the list and notifies any waiting consumers. """
        async with self.condition:
            self.actors.append(actor)
            self.condition.notify(1)

    async def take(self) -> Any:
        """ Takes the first available actor. If empty, waits for an actor to be appended. """
        async with self.condition:
            if not self.actors:
                await self.condition.wait()
            return self.actors.pop(0)

    def size(self) -> int:
        """ Returns the count of actors in the list. """
        return len(self.actors)


class LoadBalancer:
    """
    Tries to submit the request to the calculator which already has a warm cache for the given request.
    If there are multiple calculators available, gets the first one.
    Creates new calculators for newly seen requests.
    Can create actors upfront for heavy requests.
    """

    actors: dict[str, ActorsList]
    cluster_client: ClusterClient
    lock: asyncio.Lock

    def __init__(self, client: ClusterClient) -> None:
        self.actors = {}
        self.cluster_client = client
        self.lock = asyncio.Lock()

    def __str__(self) -> str:
        return "Actors in load balancer: " + ",".join([k + "=" + str(v.size()) for k, v in self.actors.items()])

    async def create_actors_for_heavy_requests(self) -> None:
        """ Creates more actors upfront for heavy requests. """

        logging.info("Creating actors for heavy requests...")
        londonflow_coros = [self.__create_new_actor() for _ in range(20)]
        tmsfx_coros = [self.__create_new_actor() for _ in range(20)]

        # TODO: Handle exceptions well
        # Join all coroutines into one list and execute in parallel.
        all_actors = await asyncio.gather(*(londonflow_coros + tmsfx_coros))

        # Need to pick parts of the joined list of all actors.
        self.actors["LONDONFLOW"] = ActorsList(all_actors[0:20])
        self.actors["TMS FX"] = ActorsList(all_actors[20:40])

        logging.info("Actors created.")

    async def __create_new_actor(self) -> Any:
        logging.debug("Creating actor in load balancer...")
        risk_calculator = await self.cluster_client.create_actor()
        return risk_calculator

    async def __get_or_create_new_actor(self, request_name: str) -> Any:
        await self.lock.acquire()
        if not request_name in self.actors:
            self.actors[request_name] = ActorsList()
            self.lock.release()  # Release the lock so that others don't have to wait for new actors
            # TODO: Handle exceptions well
            new_actors = await asyncio.gather(
                self.__create_new_actor(),
                self.__create_new_actor())
            # Add the second to the list and take the first one for ourselves
            await self.actors[request_name].append(new_actors[1])
            return new_actors[0]
        self.lock.release()
        return await self.actors[request_name].take()

    async def submit(self, request: RiskRequest) -> RiskResponse:
        """ Find a suitable actor and submit the request to it. """
        logging.debug(request)
        actor: Any = await self.__get_or_create_new_actor(request.batch_name)
        try:
            response: RiskResponse = await self.cluster_client.submit_request(request, actor)
            logging.debug(response)
            return response
        finally:
            await self.actors[request.batch_name].append(actor)
