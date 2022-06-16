"""Contains methods for fetching, appending, and reporting issues with,
characteristic data."""
import logging
import configparser
import datetime as dt

import pandas as pd

import src.utilities.config_tools as ct

def get_characteristics(config: configparser.ConfigParser):
    """
    Reads the manually created characteristic data.

    Parameters
    ----------
    config : configparser.ConfigParser
        Contents of config file

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

    default_columns = ct.cfg_get_list(config, 'PARSER', 'characteristic_merge')
    backup_columns = ct.cfg_get_list(config, 'PARSER',
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
