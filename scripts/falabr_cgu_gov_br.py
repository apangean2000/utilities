"""
Analysis of data requests/complaints from interested parties at falabr.cgu.gov.br
"""

import io
import json
import os
import re
import sys
import zipfile
from pathlib import Path
from typing import Type, Optional

import pandas as pd
import requests
from tqdm import tqdm

from pandas_profiling import ProfileReport

URL_BASE = "https://falabr.cgu.gov.br/publico/DownloadDados/"
DATA_DIRECTORY = f"data/{sys.argv[0].split('/')[-1]}"


def clear_data(directory: Path) -> None:
    """
    Clear data directory
    """

    directory = Path(directory)
    for item in directory.iterdir():
        if item.is_dir():
            clear_data(item)
        else:
            item.unlink()


def create_dict_key(dct: dict, key: str, typ: Type = list) -> dict:
    """
    dict_list _summary_

    :param dct: _description_
    :type dct: dict
    :param key: _description_
    :type key: str
    :return: _description_
    :rtype: dct
    """

    if typ == list:

        if key not in dct:
            dct[key] = []

    else:
        raise Exception("Not implemented")

    return dct


def get_data() -> None:
    """
    get_data _summary_
    """

    clear_data(Path(DATA_DIRECTORY))

    os.makedirs(DATA_DIRECTORY, exist_ok=True)

    txt_base = requests.get(f"{URL_BASE}/DownloadDadosLai.aspx").text

    headers: Optional[list[str]] = []
    file_headers: dict = {}

    for _ in [
        "Dicionário de Dados dos Relatórios de Pedidos",
        "Dicionário de Dados de Solicitantes",
        "Dicionário de Dados dos Recursos e Reclamações",
    ]:

        headers.append(re.search(f'href="([^"]+).+?{_}.*', txt_base).group(1))

    for _ in headers:

        if _ not in file_headers:
            file_headers[_] = {}

        txt = requests.get(f"{URL_BASE}{_}").text

        idx = 0

        for line in txt.splitlines():

            datadict_re = re.compile(
                "- (?P<fieldname>.+?) - (?P<format>.+?): (?P<description>.+?)$"
            )

            for fieldset in [m.groupdict() for m in datadict_re.finditer(line)]:
                file_headers[_][idx] = {k: v.strip() for k, v in fieldset.items()}
                idx += 1

    with open(f"{DATA_DIRECTORY}/data_dictionary.json", "w") as f:
        json.dump(file_headers, f, ensure_ascii=False)

    # js postbacks etc so just extract the years and get the data statically

    years = set(re.findall(r">(\d{4})", txt_base))

    for year in years:

        data = requests.get(
            f"https://dadosabertos-download.cgu.gov.br/FalaBR/Arquivos_FalaBR/Pedidos_csv_{year}.zip"
        ).content

        z = zipfile.ZipFile(io.BytesIO(data))
        z.extractall(f"{DATA_DIRECTORY}/Pedidos_csv")

        data = requests.get(
            f"https://dadosabertos-download.cgu.gov.br/FalaBR/Arquivos_FalaBR/Recursos_Reclamacoes_csv_{year}.zip"
        ).content

        z = zipfile.ZipFile(io.BytesIO(data))
        z.extractall(f"{DATA_DIRECTORY}/Recursos_Reclamacoes_csv")


def create_eda() -> None:
    """
    Create a oneliner EDA summary of data
    """

    # If header file doesn't exist get data from website

    os.makedirs(DATA_DIRECTORY, exist_ok=True)

    data_dict = f"{DATA_DIRECTORY}/data_dictionary.json"

    if os.path.isfile(data_dict) is False:
        get_data()

    with open(data_dict) as f:
        headers_json = {
            k.replace("-Formato.txt", ""): v for k, v in json.loads(f.read()).items()
        }

    dfs: dict = {}

    latest = max(int(str(file)[-8:-4]) for file in Path(DATA_DIRECTORY).rglob("*.csv"))

    for file in Path(DATA_DIRECTORY).rglob("*.csv"):

        filename = (str(file).split("/"))[-1]

        date_str = filename[:8]

        for _ in headers_json.keys():

            filetype = filename[9:].split("_")[0]

            if re.search(f"^{_}", filetype):

                dfs = create_dict_key(dfs, filetype)

                names = [_["fieldname"] for _ in headers_json[_].values()]

                # Hack for some ragged columns not spec'd in data dictionary, or with delimiter not enclosed with quoting
                names.extend(["ragged1", "ragged2", "ragged3"])

                df = pd.read_csv(file, names=names, delimiter=";", encoding="UTF-16")

                dfs[filetype].append(df)

                # Output latest year with most current changes
                if int(str(file)[-8:-4]) == latest:

                    filetype_yyyy = f"{filetype}_{str(file)[-8:-4]}"
                    dfs = create_dict_key(dfs, filetype_yyyy)
                    dfs[filetype_yyyy].append(df)


    docs_dir = f"{DATA_DIRECTORY}/docs"

    os.makedirs(docs_dir, exist_ok=True)

    for _ in tqdm(dfs):

        df = pd.concat(dfs[_])
        profile = ProfileReport(
            df,
            title=f"Relatório de perfil {_}, data {date_str}",
            explorative=True,
            # Use accessible palette
            plot={"correlation": {"cmap": "viridis", "bad": "#000000"}},
        )

        profile.to_file(f"{docs_dir}/{_}.html")

        with open(f"{docs_dir}/{_}.json", 'w') as file:
            json.dump(profile.to_json(), file)


if __name__ == "__main__":

    create_eda()
