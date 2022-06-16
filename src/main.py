"""Main point of entry."""
import logging
import configparser
import json
import os
import datetime as dt

import pandas as pd

import src.utilities.config_tools as ct
import src.utilities.json_tools as jt
import src.dataproc.characteristics as char
from src.utilities.api_tools import raise_request


def scrape_mp_data(config: configparser.ConfigParser):
    """
    Fetches a copy of the raw MP data, from gov.uk.

    Parameters
    ----------
    config : configparser.ConfigParser
        Contents of config file.
    Returns
    -------
    pd.DataFrame
        Full MP data, from api.
    """
    logging.debug('Getting data from api')

    json_path = config['PARSER']['mp_json_path']
    url = config['PARSER']['members_url']

    r = raise_request(url, headers=ct.cfg_get_dict(config, 'PARSER',
                                                   'headers'))

    data, r = jt.jsonify_response(r)
    jt.write_to_json(data, json_path)

    full_member_data = jt.json_to_pd(data['Members']['Member'],
                                     cols={'@Member_Id': 'int64'})

    return full_member_data


def read_local_mp_data(config: configparser.ConfigParser):
    """
    Fetches a copy of the raw MP data, from local copy created previously.

    Parameters
    ----------
    config : configparser.ConfigParser
        Contents of config file.

    Returns
    -------
    pd.DataFrame
        Full MP data, from local.
    """
    logging.debug('Getting data from file')

    json_path = config['PARSER']['mp_json_path']

    def data_exists(path): return os.path.exists(path)
    assert data_exists(json_path), \
        logging.error('File {0} missing. Rerun in scrape mode by '
                      'setting scrape_mode in config'.format(json_path))

    with open(json_path) as f:
        data = json.load(f)

    full_member_data = jt.json_to_pd(data['Members']['Member'],
                                     cols={'@Member_Id': 'int64'})

    return full_member_data


def get_mp_data(config: configparser.ConfigParser):
    """
    Fetches a copy of the raw MP data, either from gov.uk or from local copy
    created previously.

    Parameters
    ----------
    config : configparser.ConfigParser
        Contents of config file.

    Returns
    -------
    pd.DataFrame
        Full MP data, from either api or local depending on config settings.
    """
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
    """
    Clean raw MP data:
    - Keep only needed columns
    - Create a clean name column without titles or honourifics

    Parameters
    ----------
    config : configparser.ConfigParser
        Contents of config file.
    full_member_data : pd.DataFrame
        Unclean MP data, with many superfluous columns.

    Returns
    -------
    pd.DataFrame
        Cleaned MP data.

    """
    # Keep only desired columns.
    mp_cols = ct.cfg_get_list(config, 'PARSER', 'mp_cols')
    core_mp_data = full_member_data[mp_cols]

    # Create clean name column - no titles or honourifics.
    titles = ct.cfg_get_list(config, 'PARSER', 'titles')
    core_mp_data['clearName'] = core_mp_data['DisplayAs'].str.replace(
        '|'.join(titles), '').str.strip()

    logging.debug('Cut down to core shape: {0} and core columns: {1}'.format(
        core_mp_data.shape, core_mp_data.columns))

    return core_mp_data


def main(config: configparser.ConfigParser):
    """
    Begins main system processing.

    Currently:
    - Gets MP data
    - Cleans it appropriately
    - Fetches characteristic data, and appends to MP data.
    - Exports

    Parameters
    ----------
    config: configparser.ConfigParser
        Contents of a config file for use in main system
    """
    logging.info('Starting main process')

    full_member_data = get_mp_data(config)

    core_mp_data = clean_mp_data(config, full_member_data)

    characteristic_data = char.get_characteristics(config)
    core_mp_data = char.add_characteristics(config, core_mp_data,
                                            characteristic_data)

    core_mp_data.to_csv(config['PARSER']['full_mp_path'], header=True,
                        index=False)


def check_for_errors():
    """
    Posts warning messages if any warning files exist.

    Looks for all files within specified list of error files from the config.
    Lists any files that exist, the last modification time, and the number of
    records contained within.
    """
    files = ct.cfg_get_list(config, 'ERROR_CHECKING', 'files')

    files = [file for file in files if os.path.exists(file)]

    if len(files) > 0:
        logging.warning('Following issue files exist:')

        for file in files:
            mod_time = dt.datetime.utcfromtimestamp(os.path.getmtime(file))
            logging.warning("{0}, modified: {1}".format(file, mod_time))

            issue = pd.read_csv(file)
            logging.warning("Contains {0} records".format(len(issue)))

        logging.warning("For modification times occurring within last run, "
                        "check files for relevant issues.")


if __name__ == "__main__":

    config = configparser.ConfigParser(delimiters="=")
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
