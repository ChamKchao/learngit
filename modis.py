import requests
from requests.packages.urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

from requests_toolbelt import sessions

import json

from multipledispatch import dispatch

SUBSET = 'subset'
DATES = 'dates'

DEFAULT_TIMEOUT = 5  # seconds


class TimeoutHTTPAdapter(HTTPAdapter):
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)


http = sessions.BaseUrlSession(base_url='https://modis.ornl.gov/rst/api/v1/')

http.mount("https://",
           TimeoutHTTPAdapter(
               timeout=10,
               max_retries=Retry(total=3, backoff_factor=1,
                                 status_forcelist=[
                                     429, 500, 502, 503, 504],
                                 method_whitelist=["GET"])))


# Use following for a csv response: header = {'Accept': 'text/csv'}
header = {'Accept': 'application/json'}


def getDates(product, latitude, longitude):
    modisResponse = http.get(''.join([
        product, '/', DATES, '?',
        'latitude=', str(latitude),
        '&longitude=', str(longitude)
    ]), headers=header)
    return json.loads(modisResponse.text)


@dispatch(str, float, float, str, str, int, int)
def getSubset(product, latitude, longitude, startDate, endDate, kmAboveBelow, kmLeftRight):
    modisResponse = http.get(''.join([
        product, '/', SUBSET, '?',
        'latitude=', str(latitude),
        '&longitude=', str(longitude),
        # '&band=', band,
        '&startDate=', startDate,
        '&endDate=', endDate,
        '&kmAboveBelow=', str(kmAboveBelow),
        '&kmLeftRight=', str(kmLeftRight)
    ]), headers=header)
    return json.loads(modisResponse.text)


@dispatch(str, str, float, float, str, str, int, int)
def getSubset(product, band, latitude, longitude, startDate, endDate, kmAboveBelow, kmLeftRight):
    modisResponse = http.get(''.join([
        product, '/', SUBSET, '?',
        'latitude=', str(latitude),
        '&longitude=', str(longitude),
        '&band=', band,
        '&startDate=', startDate,
        '&endDate=', endDate,
        '&kmAboveBelow=', str(kmAboveBelow),
        '&kmLeftRight=', str(kmLeftRight)
    ]), headers=header)
    return json.loads(modisResponse.text)
