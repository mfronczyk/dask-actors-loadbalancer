"""
Pricing definition.
"""

""" Marker to separate requests/responses in a stream. """
SEPARATOR = '--- PRICING SEPARATOR ---'


class PricingRequest(object):
    """ Pricing request. Needs to inherit from object explicitly because it's used in Python 2.7. """

    def __init__(self, name, trade):
        self.name = name
        self.trade = trade


class PricingResponse(object):
    """ Pricing response. Needs to inherit from object explicitly because it's used in Python 2.7. """

    def __init__(self, request, status):
        self.request = request
        self.status = status
