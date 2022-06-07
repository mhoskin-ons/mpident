import requests
import pandas as pd

import configparser
import logging
import json
import os
import datetime as dt

from src.config_tools import *


def raise_request(url: str, headers: dict = None) -> requests.Response:
    """

    Parameters
    ----------
    url : str
        Address to access
    headers : dict
        Header values to pass into requests.get to alter the response from
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


def get_characteristics():
    """
    Reads the manually created characteristic data.
    Returns
    -------
    pd.DataFrame
        Characteristic data read from file
    """
    logging.debug('Reading characteristic data from {0}'.format(
        config['PARSER']['characteristic_path']))

    characteristic_data = pd.read_csv(config['PARSER']['characteristic_path'])

    return characteristic_data


def report_missing_characteristics(missing_characteristics: pd.DataFrame):
    """
    Takes a dataframe which is missing matching characteristics, and writes
    the details to a file.

    Parameters
    ----------
    missing_characteristics : pd.DataFrame
        DataFrame of records missing from characteristic data.
    """
    logging.warning("Missing characteristics for {0} MPs in {1}".format(
        len(missing_characteristics), config['PARSER']['characteristic_path']))

    now = dt.datetime.now().isoformat()
    warn_path = "logs/missing_identifiers.csv".format(now)
    logging.warning("Writing details of missing data to {0}".format(warn_path))

    key_details = missing_characteristics[['@Member_Id', 'DisplayAs', 'ListAs',
                                           'MemberFrom', 'Party.#text']]
    key_details.insert(0, 'timestamp', now)
    key_details.insert(1, 'missing', 'identifiers.csv')

    key_details.to_csv(warn_path, index=False)


def add_characteristics(member_data: pd.DataFrame,
                        characteristic_data: pd.DataFrame):

    logging.debug('Adding characteristic data to core data')

    merged_data = pd.merge(member_data, characteristic_data,
                           on=cfg_get_list(config, 'PARSER',
                                           'characteristic_merge'),
                           how='left', indicator=True)

    missing_characteristics = merged_data[merged_data['_merge'] == 'left_only']
    missing_characteristics = missing_characteristics[['@Member_Id']]

    missing_member_data = member_data[member_data['@Member_Id'].isin(
        missing_characteristics['@Member_Id'])]

    missing_member_data = pd.merge(missing_member_data, characteristic_data,
                                   on=cfg_get_list(config, 'PARSER',
                                                   'characteristic_merge_excess'),
                                   how='left',
                                   indicator=True)

    missing_characteristics = (missing_member_data[
                                missing_member_data['_merge'] == 'left_only'])
    matched_members = pd.concat([merged_data, missing_member_data])

    if not missing_characteristics.empty:
        report_missing_characteristics(missing_characteristics)

    return member_data


def json_to_pd(json_data: list, cols: dict, max_level=1):

    pd_data = pd.json_normalize(json_data, max_level=1)

    for col, col_type in cols.items():
        pd_data[col] = pd_data[col].astype(col_type)

    return pd_data


def scrape_mp_data(config: configparser.ConfigParser):
    logging.debug('Getting data from api')

    json_path = config['PARSER']['mp_json_path']
    url = config['PARSER']['members_url']

    r = raise_request(url, headers=cfg_get_dict(config, 'PARSER', 'headers'))

    data, r = jsonify_response(r)
    write_to_json(data, json_path)

    full_member_data = json_to_pd(data['Members']['Member'],
                                  cols={'@Member_Id': 'int64'})

    return full_member_data


def read_local_mp_data(config: configparser.ConfigParser):
    logging.debug('Getting data from file')

    json_path = config['PARSER']['mp_json_path']

    def data_exists(path): return os.path.exists(path)

    assert data_exists(json_path), \
        logging.error('File {0} missing. Rerun in scrape mode by '
                      'setting scrape_mode in config'.format(json_path))

    with open(json_path) as f:
        data = json.load(f)

    full_member_data = json_to_pd(data['Members']['Member'],
                                  cols={'@Member_Id': 'int64'})

    return full_member_data


def get_mp_data(config: configparser.ConfigParser):

    scrape_mode = config['PARSER']['scrape_mode']

    if scrape_mode == 'True':
        full_member_data = scrape_mp_data(config)
    else:
        full_member_data = read_local_mp_data(config)

    logging.debug('Found full mp data of shape: {0}'.format(
        full_member_data.shape))

    return full_member_data


def clean_mp_data(config: configparser.ConfigParser,
                  full_member_data: pd.DataFrame):

    # Keep only desired columns.
    mp_cols = cfg_get_list(config, 'PARSER', 'mp_cols')
    core_mp_data = full_member_data[mp_cols]

    # Create clean name column - no titles or honourifics.
    titles = cfg_get_list(config, 'PARSER', 'titles')
    core_mp_data['clearName'] = core_mp_data['DisplayAs'].str.replace(
        '|'.join(titles), '').str.strip()

    logging.debug('Cut down to core shape: {0} and core columns: {1}'.format(
        core_mp_data.shape, core_mp_data.columns))

    return core_mp_data

def main(config: configparser.ConfigParser):

    logging.info('Starting main process')

    # scrape_mode = config['PARSER']['scrape_mode']
    # json_path = config['PARSER']['mp_json_path']

    full_member_data = get_mp_data(config)

    core_mp_data = clean_mp_data(config, full_member_data)

    characteristic_data = get_characteristics()
    core_mp_data = add_characteristics(core_mp_data, characteristic_data)



    core_mp_data.to_csv(config['PARSER']['full_mp_path'], header=True,
                        index=False)


def check_for_errors():
    """
    Posts warning messages if any warning files exist.
    """
    files = cfg_get_list(config, 'ERROR_CHECKING', 'files')

    files = [file for file in files if os.path.exists(file)]

    if len(files) > 0:
        logging.warning('Following issue files exist:')

        for file in files:
            mod_time = dt.datetime.utcfromtimestamp(os.path.getmtime(file))
            logging.warning("{0}, modified: {1}".format(file, mod_time))

            issue = pd.read_csv(file)
            logging.warning("Contains {0} records".format(len(issue)))

        logging.warning("For times occurring within last run, check files for "
                        "issues.")


if __name__ == "__main__":

    config = configparser.ConfigParser(delimiters=("="))
    config.read('dev_mpident.ini')

    logging.basicConfig(
        level=logging.getLevelName(config['LOGGING']['level']),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(config['LOGGING']['file'])]
    )
    logging.info('begin')

    main(config)

    check_for_errors()

    logging.info('fin')
