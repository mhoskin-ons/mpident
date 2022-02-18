import requests
import pandas as pd

from typing import Callable
import configparser
import logging
import json
import os
import datetime as dt

def cfg_get_dict(config: configparser.ConfigParser,
                 section: str,
                 variable: str):

    logging.debug('Parsing config value as a dict: {0}:{1}'.format(section,
                                                                   variable))
    value = config[section][variable]
    init_split = value.split(',')
    split_pairs = [pair.split(': ') for pair in init_split]
    dict_value = {k.strip(): v.strip() for k,v in split_pairs}

    return dict_value

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


def raise_request(url, headers):

    r = requests.get(url, headers)

    if r.status_code != 200:
        logging.warning('Error code {0} from {1}'.format(r.status_code, url))
        r = None

    return r


def main(config: configparser.ConfigParser):

    logging.info('Starting main process')
    url = config['PARSER']['members_url']
    # response = requests.get("http://data.parliament.uk/membersdataplatform/services/mnis/members/query/House=Commons%7CIsEligible=true/")
    r = requests.get(url, headers=cfg_get_dict(config, 'PARSER','headers'))
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

    r.encoding = 'utf-8-sig'
    data = r.json()
    with open('mps.json', 'w') as f:
        json.dump(data, f, indent=2)

    expanded_table = pd.json_normalize(data['Members']['Member'], max_level=1)
    logging.debug(expanded_table.shape)
    core_mp_data = expanded_table[mp_cols]
    logging.debug(core_mp_data.columns)
    logging.debug(core_mp_data.shape)

    pd.set_option("display.max_columns", None)
    print(core_mp_data.head())

    get_head_shots(config, core_mp_data)


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

def get_head_shots(config, data):

    logging.info('Beginning headshot api scrape.')

    base_image_url = config['PARSER']['image_base_url']
    hex_dict = cfg_get_dict(config, 'PARSER', 'image_bytes')

    for index, row in data.iterrows():
        id = row['@Member_Id']

        name = row['ListAs']
        logging.debug('Storing image for {0}_{1}'.format(name, id))
        clean_name = name.replace(' ', '_').replace(',', '')

        member_url = base_image_url + id

        r = requests.get(member_url)
        if r.status_code == 200:
            image_config = config['PARSER']['headshot_type']

            # use image_config value or get from headers
            if image_config == 'GET':
                headshot_header_type = r.headers['Content-Type'].split('/')[-1]
                logging.debug('Header suggests {0} filetype'.format(
                    headshot_header_type))

                initial_hex = r.content.hex()[:2]

                if initial_hex in hex_dict.keys():
                    headshot_hex = hex_dict[initial_hex]
                    logging.debug('Bytes suggests {0} filetype'.format(
                        headshot_hex))

                    if headshot_hex != headshot_header_type:
                        logging.warning('Mismatched type. Defaulting to '
                                        'bytes value')
                    headshot_type = headshot_hex
                else:
                    headshot_type = headshot_header_type

            else:
                headshot_type = image_config

            image_location = "../headshots/{0}_{1}.{2}".format(clean_name,
                                                               id,
                                                               headshot_type)

            with open(image_location, 'wb') as f:
                logging.debug('Writing to {0}'.format(image_location))
                f.write(r.content)

        else:
            logging.warning('Received status code {0} for {1}. File not '
                            'downloaded'.format(r.status_code, clean_name))


if __name__ == "__main__":

    config = configparser.ConfigParser(delimiters=("="))
    config.read('../dev_mpident.ini')

    logging.basicConfig(
        level=logging.getLevelName(config['LOGGING']['level']),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(config['LOGGING']['file'])]
    )
    logging.info('begin')

    main(config)

    logging.info('fin')