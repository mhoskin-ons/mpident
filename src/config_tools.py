import configparser
import logging
from typing import Callable


def cfg_get_dict(config: configparser.ConfigParser,
                 section: str,
                 variable: str):
    """
    Parses a configparser dict value. Splits by comma, splits by colon, clears
    whitespace. Unlike list variant, doesn't cast keys or values,
    so resulting dictionary will be entirely string based.

    Parameters
    ----------
    config: configparser.ConfigParser
        object containing contents of config file
    section: str
        Section of config file to explore
    variable: str
        Name of key within section to read

    Returns
    -------
    value_dict: dict
        Dictionary containing elements of config value.

    TODO
    ----
    Handle dictionary values of different types - casting etc, or of a list

    """
    logging.debug('Parsing config value as a dict: {0}:{1}'.format(section,
                                                                   variable))
    value = config[section][variable]

    # split into k-v pairs
    kv_split = value.split(',')

    # split k-v pairs - list of lists
    split_pairs = [pair.split(': ') for pair in kv_split]

    # convert list of lists into k:v, and clear whitespace
    value_dict = {k.strip(): v.strip() for k, v in split_pairs}

    return value_dict

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


if __name__ == "__main__":
    config = configparser.ConfigParser(delimiters="=")
    config.read('../dev_mpident.ini')

    logging.basicConfig(
        level=logging.getLevelName(config['LOGGING']['level']),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(config['LOGGING']['file'])]
    )
    logging.info('Beginning config demonstration')

    list_example = cfg_get_list(config, 'PARSER', 'mp_cols')
    logging.debug('Example list: {0}'.format(list_example))

    dict_example = cfg_get_dict(config, 'PARSER', 'headers')
    logging.debug('Example dict: keys: {0}, values: {1}'.format(
        dict_example.keys(), dict_example.values()))
