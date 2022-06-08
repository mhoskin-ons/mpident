import requests
import configparser
import logging
import pandas as pd
import os

from src.config_tools import *
from src.main import raise_request


def get_head_shot_type(config: configparser.ConfigParser,
                       r: requests.Response,
                       hex_dict: dict):
    """
    Gets the file type for a specific headshot request.
    If a specific type is set in the config, uses that.
    If the config is set to get, will look at the content type from the
    Response, and the first bytes - uses the content type, unless the
    first bytes are different. If no matching bytes are found, flags an
    error, and uses the content type.

    Parameters
    ----------
    config: configparser.ConfigParser
        Config file to read url etc. from
    r: requests.Response
        Data from API
    hex_dict: dict{str:str}
        Mapper of hex codes to file types

    Returns
    -------
    headshot_type: str
        Filetype of image to store
    """
    image_file_type = config['PARSER']['headshot_type']

    # use image_config value or get from headers
    if image_file_type == 'GET':
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
            logging.warning('Initial hex: {0} not found in config '
                            'file. Using header value'.format(initial_hex))
            headshot_type = headshot_header_type

    else:
        headshot_type = image_file_type

    return headshot_type


def get_head_shots(config: configparser.ConfigParser, data: pd.DataFrame):
    """
    For a specific dataframe containing unique names and ids, gets their
    parliamentary stock image and writes it to a file.

    Includes logic to try and guess the filetype of images by interrogating
    first bytes of image data.

    Parameters
    ----------
    config: configparser.ConfigParser
        Config file to read url etc. from
    data: pd.DataFrame
        DataFrame containing unique member ids and names
    """
    logging.info('Beginning headshot api scrape.')

    base_image_url = config['HEADSHOTS']['image_base_url']
    hex_dict = cfg_get_dict(config, 'HEADSHOTS', 'image_bytes')

    for index, row in data.iterrows():
        member_id = row[config['HEADSHOTS']['id_column']]
        name = row[config['HEADSHOTS']['name_column']]

        logging.debug('Storing image for {0}_{1}'.format(name, member_id))
        clean_name = name.replace(' ', '_').replace(',', '')

        member_url = base_image_url + member_id

        r = raise_request(member_url)

        if r:
            headshot_type = get_head_shot_type(config, r, hex_dict)
            image_location = "../headshots/{0}_{1}.{2}".format(clean_name,
                                                               member_id,
                                                               headshot_type)

            with open(image_location, 'wb') as f:
                logging.debug('Writing to {0}'.format(image_location))
                f.write(r.content)

        else:
            logging.warning('No usable response, so no image data for {0}_{1} '
                            'stored.'.format(clean_name, member_id))


if __name__ == "__main__":
    config = configparser.ConfigParser(delimiters="=")
    config.read('../dev_mpident.ini')

    logging.basicConfig(
        level=logging.getLevelName(config['LOGGING']['level']),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(config['LOGGING']['file'])]
    )
    logging.info('Beginning standalone head shot request. Requires mps.json '
                 'to be found in the data folder.')

    mp_data_path = '../' + config['PARSER']['mp_json_path']

    # simple local function for allowing assert to log error message
    def data_exists(path): return os.path.exists(path)

    assert data_exists(mp_data_path), \
        logging.error('{0} file not found. Run code to download data from api '
                      'and write to file first.'.format(mp_data_path))

    data = pd.read_json(mp_data_path)

    get_head_shots(config, data)
