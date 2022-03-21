"""
Get a list of mime types return as .csv and .json
"""


import re
import requests
import logging
import sys
import csv
import io
import os
import json
from copy import deepcopy

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

URL_BASE = 'https://www.iana.org/assignments/media-types/'
DATA_DIRECTORY = f"data/{sys.argv[0].split('/')[-1]}"


def get_data() -> None:
    
    url = f'{URL_BASE}media-types.xhtml'

    logging.info(f'Getting {url}')

    resp = requests.get(url)

    if resp.status_code == 200:
        
        datas = re.findall('<a class="altformat" href="([^"]+.csv)">',resp.text,re.DOTALL)

        fieldnames = ['name','id','reference','isobsolete','isdeprecated']
        data_dcts = data_dct = {}

        os.makedirs(DATA_DIRECTORY, exist_ok=True)

        with open(f"{DATA_DIRECTORY}/{sys.argv[0].split('/')[-1]}.csv", "w") as f:

            dwrite = csv.DictWriter(f, fieldnames=fieldnames)
            dwrite.writeheader()

            for url in datas:

                url = f'{URL_BASE}{url}'

                resp = requests.get(url)

                if resp.status_code == 200:

                    dreads = csv.DictReader(io.StringIO(resp.text))

                    for dread in dreads:

                        for idx, fieldname in enumerate(fieldnames[:3]):
                            data_dct[fieldnames[idx]] = dread[list(dread.keys())[idx]]

                        for idx, other_check in enumerate(['OBSOLETED','DEPRECATED'],3):

                            data_dct[fieldnames[idx]] = False

                            if re.search(other_check, dread['Name']):
                                data_dct[fieldnames[idx]] = True

                        dwrite.writerow(data_dct)

                        data_dcts[data_dct['id']] = deepcopy(data_dct)

            else:
                logging.error(f'Issue with {url} status code {resp.status_code}')
        
        with open(f"{DATA_DIRECTORY}/{sys.argv[0].split('/')[-1]}.json",'w') as f:
            json.dump(data_dcts,f) # Try log here

    else:
        logging.error(f'Issue with {url} status code {resp.status_code}')


if __name__ == "__main__":
    get_data()

