import requests
import pandas as pd

import configparser
import json
import os
import datetime as dt

from src.utilities.config_tools import *
import src.utilities.json_tools as jt


def raise_request(url: str, headers: dict = None) -> requests.Response:
    """

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

    r = raise_request(url, headers=cfg_get_dict(config, 'PARSER', 'headers'))

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
    mp_cols = cfg_get_list(config, 'PARSER', 'mp_cols')
    core_mp_data = full_member_data[mp_cols]

    # Create clean name column - no titles or honourifics.
    titles = cfg_get_list(config, 'PARSER', 'titles')
    core_mp_data['clearName'] = core_mp_data['DisplayAs'].str.replace(
        '|'.join(titles), '').str.strip()

    logging.debug('Cut down to core shape: {0} and core columns: {1}'.format(
        core_mp_data.shape, core_mp_data.columns))

    return core_mp_data


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


def report_missing_characteristics(config: configparser.ConfigParser,
                                   missing_characteristics: pd.DataFrame):
    """
    Takes a dataframe which is missing matching characteristics, and writes
    the details to a file.

    Writes to a missing_identifiers log csv, and optionally if config value is
    set, can also append the known data about the unknown MP to the default
    identifiers csv.

    Parameters
    ----------
    config : configparser.ConfigParser
        Contents of config file
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

    key_details.to_csv(warn_path, mode='a', index=False)

    if config['PARSER']['append_missing_characteristics'] == "True":
        append_details = missing_characteristics[["@Member_Id", "DisplayAs",
                                                  "ListAs", "clearName"]]
        append_path = config['PARSER']['characteristic_path']

        logging.warning("Appending missing characteristic records to default "
                        "characteristic data - will need expanding with "
                        "user-filled values.")
        append_details.to_csv(append_path, mode='a', index=False, header=False)


def add_characteristics(config: configparser.ConfigParser,
                        member_data: pd.DataFrame,
                        characteristic_data: pd.DataFrame):
    """
    Appends characteristics to members, where possible.

    Will first merge on columns specified in characteristic_merge in config,
    where failures occur will attempt to further merge on columns in
    characteristic_merge_excess.

    Resulting data will include successful matches from the first join,
    and all records from the result of the second - successes OR failures -
    note that this might mean some rows are missing characteristics. They
    will be logged and handled in report_missing_characteristics.

    Parameters
    ----------
    config : configparser.ConfigParser
        Contents of config file
    member_data : pd.DataFrame
        MP member data from either scraped gov website, or local copy.
    characteristic_data : pd.DataFrame
        Characteristic data from compiled identifiers.csv file

    Returns
    -------
    pd.DataFrame:
        MP data with matching characteristic data where possible. If no
        matching characteristic data will be found, MP will be included, but
        characteristic columns will be blank
    """

    logging.debug('Adding characteristic data to core data')

    default_columns = cfg_get_list(config, 'PARSER', 'characteristic_merge')
    backup_columns = cfg_get_list(config, 'PARSER',
                                  'characteristic_merge_excess')

    primary_merge = pd.merge(member_data, characteristic_data,
                             on=default_columns, how='left', indicator=True)

    # Split matched + unmatched out.
    missing_characteristics = primary_merge[primary_merge['_merge'] ==
                                            'left_only']
    primary_merge = primary_merge[primary_merge['_merge'] == 'both']

    # Get original data where failed to match
    missing_member_data = member_data[member_data['@Member_Id'].isin(
        missing_characteristics['@Member_Id'])]

    # Drop columns which will duplicate for backup merge
    characteristic_data.drop(columns=['DisplayAs', 'ListAs'], inplace=True)

    # Merge on backup columns
    secondary_merge = pd.merge(missing_member_data, characteristic_data,
                               on=backup_columns, how='left', indicator=True)

    # New missing_characteristics can overwrite previous
    missing_characteristics = (secondary_merge[
                                secondary_merge['_merge'] == 'left_only'])

    matched_members = pd.concat([primary_merge, secondary_merge])

    if not missing_characteristics.empty:
        report_missing_characteristics(config, missing_characteristics)

    return matched_members


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

    characteristic_data = get_characteristics()
    core_mp_data = add_characteristics(config, core_mp_data,
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
    files = cfg_get_list(config, 'ERROR_CHECKING', 'files')

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
