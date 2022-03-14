"""
    Enhance ckan extension catalogue data with github actitivites output to csv
"""

import requests
import re
import sys
import csv
import os

URL_MAIN = 'https://extensions.ckan.org'
DATA_DIRECTORY = f"data/{sys.argv[0].split('/')[-1]}"


def get_ckan():

    os.makedirs(DATA_DIRECTORY, exist_ok=True)

    datas_lst = []

    res = requests.get(URL_MAIN)

    urls = re.findall('data-url="([^"]+)',res.text,re.DOTALL)

    for url in urls:

        res = requests.get(f'{URL_MAIN}{url}')

        datas = re.search('<h1>(?P<name>.+?)<br />.*?<small>(?P<publisher>[^<]+)<.*?title="Project Home Page".*?href="(?P<url>[^"]+)',res.text,re.MULTILINE|re.DOTALL)

        if datas:
            dct_tmp = {k:v.strip() for k,v in datas.groupdict().items()}
            dct_tmp['publisher'] = re.sub('by ','',dct_tmp['publisher'])
            datas_lst.append(dct_tmp)
            #break

    for idx, _ in enumerate(datas_lst):

        url = _.get('url')
        if re.search('http',url) is None:
            continue
        print (url)
        res = requests.get(url)
        txt = res.text


        for stat in ['star','watching','fork']:
        
            data = re.search(f'<strong>(\d+)</strong>[^<]*{stat}',txt)

            if data:
                _[stat]  = data.group(1).strip()
            else:
                _[stat] = 0


        for stat in ['Used by','Contributors']:

            data = re.search(f'{stat}.*?class="Counter">(\d+)',txt)

            if data:
                _[stat]  = data.group(1).strip()
            else:
                _[stat] = 0

        url_commit = re.search('<include-fragment src="([^"]+/spoofed_commit_check/[^"]+)"',txt,re.M|re.DOTALL)

        if url_commit:
            url = f"https://github.com{url_commit.group(1)}".replace('/spoofed_commit_check/','/tree-commit/')
            res = requests.get(url)
            txt = res.text
            data = re.search('<relative\-time datetime="([^"]+)"',txt,re.DOTALL)
            _['date'] = data.group(1).strip()

        datas_lst[0] = _
        datas_lst[idx] = _

    with open(f"{DATA_DIRECTORY}/{sys.argv[0].split('/')[-1]}.csv", 'w') as f:
        dw = csv.DictWriter(f,fieldnames=list(datas_lst[0].keys()))
        dw.writeheader()
        dw.writerows(datas_lst) 


if __name__ == "__main__":
    get_ckan()
