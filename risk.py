"""
Risk requests and responses definition for the pricing service.
"""


class RiskRequest:
    """ Defines the pricing request. """

    batch_name: str
    trade: str

    def __init__(self, batch_name: str, trade: str) -> None:
        self.batch_name = batch_name
        self.trade = trade

    def __str__(self) -> str:
        return "Risk request - " + self.batch_name + ", trade " + self.trade


class RiskResponse:
    """ Defines the pricing response """

    request: RiskRequest
    status: str

    def __init__(self, request, status) -> None:
        self.request = request
        self.status = status

    def __str__(self) -> str:
        return "Risk response for {" + str(self.request) + "}, status " + self.status
