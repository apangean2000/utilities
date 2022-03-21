"""
Get a list of file formats from wikipadeia
"""

import csv
import os
import re
import sys
import logging
import requests

logging.basicConfig(stream=sys.stdout, level=logging.WARNING)

URL_MAIN = "https://en.wikipedia.org/wiki/List_of_file_formats"
DATA_DIRECTORY = f"data/{sys.argv[0].split('/')[-1]}"


def get_data() -> None:
    """
    Get list of file formats with extension and description
    """

    resp = requests.get(URL_MAIN)
    
    if resp.status_code != 200:
        logging.error(f"URL: {URL_MAIN} status code {str(resp.status_code)}")

    os.makedirs(DATA_DIRECTORY, exist_ok=True)

    with open(f"{DATA_DIRECTORY}/{sys.argv[0].split('/')[-1]}.csv", "w") as f:

        dw = csv.DictWriter(f, fieldnames=['extension','description'])
        dw.writeheader()

        for _ in re.findall('<li>(.+?)<li>',resp.text,re.M|re.DOTALL):

            try:
                datas = re.search('^(.+?)(?:-|–)(.*)$',re.sub('<[^<]+?>', '', _),flags=re.M|re.DOTALL).groups()
            except:
                datas = []

            if len(datas) == 2:

                # Just get first enclosing brackets here

                if enclosed_datas := re.findall('\(([^\)]+)\).*',datas[0]):

                    for enclosed_data in enclosed_datas[0].split(','):

                        if re.search('\[edit\]',datas[1].strip(),re.M):
                            logging.info(f"skipped {datas}")
                            continue

                        data = {'extension':    enclosed_data.strip(),
                                'description':  datas[1].strip()}

                        dw.writerow(data)

                else:

                    for data_splt in datas[0].split(','):

                        if re.search('\[edit\]',datas[1].strip(),re.M):
                            logging.info(f"skipped {datas}")
                            continue

                        data = {'extension'    :  data_splt.strip(),
                                'description'  :  datas[1].strip()}

                        dw.writerow(data)

            else:
                logging.info(f"skipped {datas}")


if __name__ == "__main__":
    get_data()