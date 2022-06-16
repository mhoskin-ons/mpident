# Contains methods for dealing with json data.

import requests
import json
import logging
import os
import datetime as dt
import pandas as pd


def jsonify_response(r: requests.Response, encoding: str = 'utf-8-sig'):
    """
    Takes a requests.Response object, encodes to utf-8-sig by default,
    and returns a dictionary containing the data in a json format

    Parameters
    ----------
    r : requests.Response
        Response object from requests module, containing json data.
    encoding : str
        Specific encoding for received data.
        The default is 'utf-8-sig'.

    Returns
    -------
    tuple(data, r)
        Data is the contents converted to a json format - stored in a
        dictionary several levels deep.
        r is the requests.Response value (encoded)
    """
    logging.debug('Converting response data to json style data as a dict')
    r.encoding = encoding
    data = r.json()

    return data, r


def write_to_json(data: dict,
                  file_name: str,
                  write_mode: str = 'w'):
    """
    Takes Response, encodes, pulls out data as json and writes to file
    Parameters
    ----------
    data: dict
        Response data converted to a dict storing json style data
    file_name: str
        File name to write out data as.
    write_mode: str
        Opens file_name in specific mode.
        The default is 'w'
    """

    logging.info('Dumping to {0}'.format(file_name))

    with open(file_name, write_mode) as f:
        json.dump(data, f, indent=2)

    exist_status = os.path.exists(file_name)

    if exist_status:
        time = dt.datetime.utcfromtimestamp(os.path.getmtime(file_name))
        logging.debug('Dump complete. Status unknown: File exists, '
                      'last modified: {0}.'.format(time))
    else:
        logging.warning('Dump failed. Status unknown - File does not exist.')


def json_to_pd(json_data: list, cols: dict, max_level=1):

    pd_data = pd.json_normalize(json_data, max_level=max_level)

    for col, col_type in cols.items():
        pd_data[col] = pd_data[col].astype(col_type)

    return pd_data
