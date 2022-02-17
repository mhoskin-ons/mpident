import configparser
import logging
import requests
import pandas as pd
import urllib3
import lxml
import xml.etree.ElementTree as ET

def main(config: configparser.ConfigParser):

    url = config['PARSER']['url']
    response = requests.get("http://data.parliament.uk/membersdataplatform/services/mnis/members/query/House=Commons%7CIsEligible=true/")

    logging.info('request with status: {0}'.format(response.status_code))
    # print(type(response))
    # print(response.content)

    root = ET.fromstring(response.text)
    tree = ET.ElementTree(root)
    tree.write('raw_output.xml')

    mp_data = pd.read_xml(response.content, parser='etree')

    # core_mp_data = mp_data[]

    print(type(config['PARSER']['test']))
    mp_cols = config['PARSER']['mp_cols'].split(',\n')
    'http://data.parliament.uk/membersdataplatform/services/mnis/members' \
    '/query/membership=all|commonsmemberbetween=2015-03-01and2022-02-17/'
    core_mp_data = mp_data[mp_cols]
    return response

if __name__ == "__main__":

    config = configparser.ConfigParser()
    config.read('../dev_mpident.ini')

    logging.basicConfig(
        level=logging.getLevelName(config['LOGGING']['level']),
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler(),
                  logging.FileHandler(config['LOGGING']['file'])]
    )

    main(config)