"""General tools for handling api calls."""
import logging

import requests


def raise_request(url: str, headers: dict = None) -> requests.Response:
    """
    Raise an api request with optional headers.

    Parameters
    ----------
    url : str
        Address to access
    headers : dict
        Header values to pass into "requests.get" to alter the response from
        the url
        The default is None

    Returns
    -------
    requests.Response
        Response object for url to pull data etc. from.

    """
    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        logging.warning('Error code {0} from {1}. Processes will '
                        'attempt to continue without this request, '
                        'but no assurance can be made about future '
                        'states'.format(r.status_code, url))
        r = None

    return r
