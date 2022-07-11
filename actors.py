""" Dask actors module. """

import ray
from pricing import PricingRequest
from risk import RiskRequest, RiskResponse
from supervisor import Supervisor


# @ray.remote  # Comment this out to use Dask
class PricingActor:
    """ Runs pricing requests. """

    supervisor: Supervisor

    def __init__(self) -> None:
        self.supervisor = Supervisor()

    async def start(self) -> None:
        """ Starts the calculator process. Needs to be called before starting any calculations. """
        await self.supervisor.ensure_start()

    def stop(self) -> None:
        """ Stops the calculator process. """
        self.supervisor.stop()

    async def calculate_risk(self, request: RiskRequest) -> RiskResponse:
        """ Start the calculation on the calculator process. """
        pricing_request = PricingRequest(request.batch_name, request.trade)
        pricing_response = await self.supervisor.calculate(pricing_request)
        return RiskResponse(request, pricing_response.status)
