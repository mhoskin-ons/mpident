import requests
import pandas as pd

from typing import Callable
import configparser
import logging
import json
import os
import datetime as dt


def cfg_get_list(config: configparser.ConfigParser,
                 section: str,
                 variable: str,
                 cast: Callable = str):
    """
    Parses a configparser list value. Splits by comma, clears whitespace,
    and casts elements to appropriate data type.

    Parameters
    ----------
    config: configparser.ConfigParser
        object containing contents of config file
    section: str
        Section of config file to explore
    variable: str
        Name of key within section to read
    cast: Callable
        Specific function to cast by.
        The default is str

    Returns
    -------
    value_list: list
        List containing elements of config value.
    """
    logging.debug('Parsing config value as a list: {0}:{1}'.format(section,
                                                                   variable))
    value = config[section][variable]
    value_list = value.split(',')
    value_list = [cast(v.strip()) for v in value_list]

    return value_list


def main(config: configparser.ConfigParser):

    logging.info('Starting main process')
    url = config['PARSER']['url']
    # response = requests.get("http://data.parliament.uk/membersdataplatform/services/mnis/members/query/House=Commons%7CIsEligible=true/")
    r = requests.get(url, headers={"Accept": 'application/json'})
    logging.info('request with status: {0}'.format(r.status_code))
    # print(type(response))
    # print(response.content)

    # root = ET.fromstring(response.text)
    # tree = ET.ElementTree(root)
    # #tree.write('raw_output.xml')

    # mp_data = pd.read_xml(response.content, parser='etree')

    # core_mp_data = mp_data[]

    # print(type(config['PARSER']['test']))
    mp_cols = cfg_get_list(config, 'PARSER', 'mp_cols')
    # mp_cols = config['PARSER']['mp_cols'].split(',\n')
    # 'http://data.parliament.uk/membersdataplatform/services/mnis/members' \
    # '/query/membership=all|commonsmemberbetween=2015-03-01and2022-02-17/'
    # core_mp_data = mp_data[mp_cols]
    # return response

    test_url = 'http://data.parliament.uk/membersdataplatform/services/mnis/HouseOverview/Commons/2012-01-01'

    r = requests.get(url, headers={'Accept': 'application/json'})
    r.encoding = 'utf-8-sig'
    data = r.json()
    with open('mps.json', 'w') as f:
        json.dump(data, f, indent=2)

    expanded_table = pd.json_normalize(data['Members']['Member'], max_level=1)
    logging.debug(expanded_table.shape)
    core_mp_data = expanded_table[mp_cols]
    logging.debug(core_mp_data.columns)
    logging.debug(core_mp_data.shape)


def write_to_json(r: requests.Response,
                  file_name: str,
                  encoding: str = 'utf-8-sig',
                  write_mode: str = 'w'):
    """
    Takes Response, encodes, pulls out data as json and writes to file
    Parameters
    ----------
    r: requests.Response
        Response object from requests module
    file_name: str
        File name to write out data as.
    encoding: str
        Specific encoding for received data.
        The default is 'utf-8-sig
    write_mode: str
        Opens file_name in specific mode.
        The default is 'w'
    """

    logging.info('Dumping to {0}'.format(file_name))
    r.encoding = encoding
    data = r.json()
    with open(file_name, write_mode) as f:
        json.dump(data, f, indent=2)

    exist_status = os.path.exists(file_name)

    if exist_status:
        time = dt.datetime.utcfromtimestamp(os.path.getmtime(file_name))
        logging.debug('Dump complete. Status unknown: File exists, '
                      'last modified: {0}.'.format(time))
    else:
        logging.warning('Dump failed. Status unknown - File does not exist.')


if __name__ == "__main__":

    config = configparser.ConfigParser(delimiters=("="))
    config.read('../dev_mpident.ini')

    logging.basicConfig(
        level=logging.getLevelName(config['LOGGING']['level']),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(config['LOGGING']['file'])]
    )

    main(config)
