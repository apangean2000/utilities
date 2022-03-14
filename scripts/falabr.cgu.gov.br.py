"""
Analysis of data requests/complaints from interested parties at falabr.cgu.gov.br
"""

import requests
import re
import io
import zipfile
import pandas as pd
import sys
import json
import os


URL_BASE = 'https://falabr.cgu.gov.br/publico/DownloadDados/'
DATA_DIRECTORY = f"data/{sys.argv[0].split('/')[-1]}"


def get_data():

    os.makedirs(DATA_DIRECTORY, exist_ok=True)

    txt_base = requests.get(f'{URL_BASE}/DownloadDadosLai.aspx').text

    headers = []
    file_headers = {}

    for _ in [  'Dicionário de Dados dos Relatórios de Pedidos',
                'Dicionário de Dados de Solicitantes',
                'Dicionário de Dados dos Recursos e Reclamações']:
        
        headers.append(re.search(f'href="([^"]+).+?{_}.*',txt_base).group(1))

    for _ in headers:

        if _ not in file_headers:
            file_headers[_] = {}

        txt = requests.get(f'{URL_BASE}{_}').text   

        idx = 0

        for line in txt.splitlines():

            datadict_re = re.compile('- (?P<fieldname>.+?) - (?P<format>.+?): (?P<description>.+?)$')

            for fieldset in [m.groupdict() for m in datadict_re.finditer(line)]:
                file_headers[_][idx] =  {k:v.strip() for k,v in fieldset.items()}
                idx += 1

    with open (f'{DATA_DIRECTORY}/data_dictionary.json','w') as f:
        json.dump(file_headers, f, ensure_ascii=False)

    # js postbacks etc so just extract the years and get the data statically

    years = set(re.findall('>(\d{4})',txt_base))

    for year in years:

        data = requests.get(f'https://dadosabertos-download.cgu.gov.br/FalaBR/Arquivos_FalaBR/Pedidos_csv_{year}.zip').content

        z = zipfile.ZipFile(io.BytesIO(data))
        z.extractall(f'{DATA_DIRECTORY}/Pedidos_csv')

        data = requests.get(f'https://dadosabertos-download.cgu.gov.br/FalaBR/Arquivos_FalaBR/Recursos_Reclamacoes_csv_{year}.zip').content

        z = zipfile.ZipFile(io.BytesIO(data))
        z.extractall(f'{DATA_DIRECTORY}/Recursos_Reclamacoes_csv')


def create_eda():

    # If header file doesn't exist get data from website

    data_dict = f'{DATA_DIRECTORY}/data_dictionary.json'
    if os.path.isfile(data_dict) is False:
        get_data()
    
    with open(data_dict) as f:
        headers_json = json.loads(f.read())

    for root, dirs, files in os.walk(DATA_DIRECTORY):

        path = root.split(os.sep)
        files = [f'{root}/{_}' for _ in files if _[-4:] == '.csv']

        for file in files:
            # TODO
            print(file.split('/')[-1][9:])
            #df = pd.read_csv()




if __name__ == "__main__":
    #get_data()
    create_eda()
