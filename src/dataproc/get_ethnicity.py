"""Uses wikipedia page for list of ethnic minority politicians in the UK to
build a clean table of MP Ethnicities"""
import logging
import configparser

import bs4
import pandas as pd
from bs4 import BeautifulSoup

from src.main import raise_request


def parse_bs_table(table: bs4.Tag):
    """
    Parses a html table e.g. from wikipedia that has been stored as a
    BeautifulSoup Tag object, and returns as a pandas dataframe. Handles
    multirow values intelligently, by forward filling as the data is
    collated for the later rows of data, where there would be otherwise no
    values.

    Parameters
    ----------
    table : bs4.Tag
        Should be a table class objects with <th>, <tr> and <td> values.

    Returns
    -------
    pd.DataFrame :
        Data stored in a usable format, with column names pulled from the
        table header.

    ToDo
    -----
    Will not handle column spans at this point. Will require some
    duplication of carrying cells down a row to carry cells across a column.
    """
    logging.debug("Parsing HTML table.")
    # First, get headers - <th> tags, and remove any empty values
    headers = [item.get_text().strip() for item in table.find_all('th')]
    headers = [h for h in headers if h != '']

    # Get current row data - split by <tr> tags
    rows = []
    for row in table.findAll("tr"):
        rows.append(row)

    # Empty list to build up cleaned row data
    row_data = []

    # Pattern is based on whether a td has a rowspan - a cell spanning
    # multiple rows causes issues with the following row as it has less
    # cells than number of headers - the spanning cells are NOT
    # automatically carried through.
    # Number is the number of cells to have a specific content - if more
    # than 1, will need to carry down.

    # Initialise for first row - no rowspans
    previous_pattern = [1] * len(headers) # 8

    # Start from index 1 - skipping headers
    for row in rows[1:]:
        # get all cell values - <td> tags
        tds = row.findAll('td')

        # only contains as many elements as new ones in the row - anything
        # spanned from previous row won't be in this list
        # i.e.: len(row_pattern) < len(headers)
        row_pattern = [int(td.attrs['rowspan']) if 'rowspan' in td.attrs else 1
                       for td in tds]

        # Creates proper row_pattern if on a spanned row with less values in
        # row_pattern than headers; will fill from previous =- 1)
        if len(row_pattern) < len(headers):
            this_row_pattern = []
            for number in previous_pattern:
                if number == 1:
                    this_row_pattern.append(row_pattern.pop(0))
                else:
                    this_row_pattern.append(number - 1)
        # if the same length, shouldn't be any rows carried from previous row
        else:
            this_row_pattern = row_pattern

        # build up actual values
        this_row = []
        for col_idx, pattern in enumerate(previous_pattern):
            if pattern == 1: # get from current row and strip whitespace
                td = tds.pop(0)
                this_row.append(td.get_text().strip())
            else: # get from last row of finished data
                this_row.append(row_data[-1][col_idx])

        # clear up iteration - append to continuing data, and update previous
        row_data.append(this_row)
        previous_pattern = this_row_pattern

    data = pd.DataFrame(data=row_data, columns=headers)

    logging.debug('HTML table parsed.')

    return data


def clean_ethnicity(data: pd.DataFrame, name_col='Name',
                    ethnicity_col='Ethnicity',
                    status_col='Reason for tenure ending'):
    """
    Takes raw ethnicity data scraped from wikipedia, removes citations,
    removes all non-current MPs, and drops unnecessary columns.

    Parameters
    ----------
    data: pd.DataFrame
    name_col: String
        Name of column containing names of individuals
    ethnicity_col: String
        Name of column containing ethnicities of individuals
    status_col: String
        Name of column containing current serving status.

    Returns
    -------
    data: pd.DataFrame
        Cleaned ethnicity data
    """
    logging.debug('Cleaning ethnicity data.')
    # Remove citations from names
    data[name_col] = data[name_col].str.split('[', expand=True)[0]

    # Remove citations from ethnicities - rarer than names
    data[ethnicity_col] = data[ethnicity_col].str.split('[', expand=True)[0]

    # Keep only current MPs - handles any multirow values where MPs have
    # held multiple constituencies, been in multiple parties etc.
    data = data[data[status_col] == 'Serving']

    # drop portrait, years
    data = data.drop(columns=['Portrait', 'Year elected', 'Year left'])

    logging.debug('Ethnicity data cleaned.')

    return data


def get_mp_ethnicity(config: configparser.ConfigParser):
    """
    Uses BeautifulSoup to scrape information from wikipedia page for MP
    ethnicities. Returns clean table of usable data for all currently serving
    MPs of an ethnic minority.

    Parameters
    ----------
    config: configparser.ConfigParser
        Contents of config file

    Returns
    -------
    ethnicity_data: pd.DataFrame
        Table containing clean ethnicity data for MPs from ethnic minorities.
    """
    logging.debug("Fetching and processing ethnicity data.")
    url = config['ETHNICITY']['url']
    res = raise_request(url)

    soup = BeautifulSoup(res.text, 'html.parser')

    section = config['ETHNICITY']['section']
    head = soup.find('li', class_="toclevel-1 tocsection-{}".format(section))
    expected_wiki_section = config['ETHNICITY']['contents_name']

    # internal function for logging message in assert response
    def check_match(f, e): return f == e

    assert check_match(head.text, expected_wiki_section), \
        logging.error('Issue with data source url - section not matching. ' 
                      '\n\t Expected:\t {0}.'
                      '\n\t Found:   \t {1}.'.format(expected_wiki_section,
                                                     head.text))

    tables = soup.find_all('table', class_=config['ETHNICITY']['table_class'])
    table = tables[int(section)-1]

    ethnicity_data = parse_bs_table(table)
    ethnicity_data = clean_ethnicity(ethnicity_data)
    return ethnicity_data


if __name__ == "__main__":

    config = configparser.ConfigParser(delimiters="=")
    config.read('dev_mpident.ini')

    logging.basicConfig(
        level=logging.getLevelName(config['LOGGING']['level']),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(config['LOGGING']['file'])]
    )
    logging.info('Beginning standalone ethnicity scrape from Wikipedia.')
    final_ethnicity_data = get_mp_ethnicity(config)

    logging.info('Found ethnicity data for {0} current MPs.'
                 .format(len(final_ethnicity_data)))
