"""
Analysis of data requests/complaints from interested parties at falabr.cgu.gov.br
"""

import io
import json
import os
import re
import sys
from pathlib import Path
from typing import Optional
from zipfile import ZipFile

import numpy as np
import pandas as pd
import requests
from tqdm import tqdm

from pandas_profiling import ProfileReport

pd.options.display.width = 0

URL_BASE = "https://falabr.cgu.gov.br/publico/DownloadDados/"
DATA_DIRECTORY = f"data/{sys.argv[0].split('/')[-1]}"


def clear_data(directory: str) -> None:
    """
    Clear data directory
    """

    directory_p = Path(directory)

    for item in directory_p.iterdir():
        if item.is_dir():
            clear_data(str(item))
        else:
            item.unlink()


def get_data() -> None:
    """
    get_data _summary_
    """

    clear_data(DATA_DIRECTORY)

    os.makedirs(DATA_DIRECTORY, exist_ok=True)

    txt_base = requests.get(f"{URL_BASE}/DownloadDadosLai.aspx").text

    headers: list[str] = []
    file_headers: dict = {}

    for _ in [
        "Dicionário de Dados dos Relatórios de Pedidos",
        "Dicionário de Dados de Solicitantes",
        "Dicionário de Dados dos Recursos e Reclamações",
    ]:
        data_re = re.search(f'href="([^"]+).+?{_}.*', txt_base)
        if data_re:
            headers.append(data_re.group(1))

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

        z = ZipFile(io.BytesIO(data))
        z.extractall(f"{DATA_DIRECTORY}/Pedidos_csv")

        data = requests.get(
            f"https://dadosabertos-download.cgu.gov.br/FalaBR/Arquivos_FalaBR/Recursos_Reclamacoes_csv_{year}.zip"
        ).content

        z = ZipFile(io.BytesIO(data))
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

    def dateparse(dates: list[str]) -> list[Optional[np.datetime64]]:
        """
        Forgiving parse list to date type list

        :param dates: List of datetype strings
        :type dates: list
        :return: _description_
        :rtype: list[Optional[datetime]]
        """

        dates_out: list[Optional[np.datetime64]] = []

        for idx, dat in enumerate(dates):

            date_bit = f"{dat[6:10]}-{dat[3:5]}-{dat[0:2]}"
            time_bit = dat[11:] if dat[11:] else "00:00:00"

            try:
                # tz agnsotic at present though https://en.wikipedia.org/wiki/Time_in_Brazil
                dates_out.append(np.datetime64(f"{date_bit}T{time_bit}"))
            except ValueError:
                dates_out.append(np.datetime64("NaT"))

        return dates_out

    latest = max(int(str(file)[-8:-4]) for file in Path(DATA_DIRECTORY).rglob("*.csv"))

    for file in Path(DATA_DIRECTORY).rglob("*.csv"):

        filename = (str(file).split("/"))[-1]

        date_str = filename[:8]

        for _ in headers_json.keys():

            filetype = filename[9:].split("_")[0]

            if re.search(f"^{_}", filetype):

                dfs.setdefault(filetype, [])

                names = [_["fieldname"] for _ in headers_json[_].values()]
                dates = [
                    _["fieldname"]
                    for _ in headers_json[_].values()
                    if _["format"][:5] == "Data "
                ]

                # Hack for some ragged columns not spec'd in data dictionary, or with delimiter not enclosed with quoting
                # """names.extend(["ragged1", "ragged2", "ragged3"])""""
                # Added warn
                # For _Pedidos_csv  file pattern looks like data dictionary columns don't align with csv's

                df = pd.read_csv(
                    file,
                    index_col=False,
                    encoding="UTF-16",
                    names=names,
                    delimiter=";",
                    on_bad_lines="warn",
                    true_values=["Sim"],  # Grab these from locale or data dict?
                    false_values=["Não"],
                    parse_dates=dates,
                    date_parser=dateparse,
                )

                df.replace(r"^\s*$", np.nan, regex=True, inplace=True)

                dfs[filetype].append(df)

                # Output latest year with most current changes
                if int(str(file)[-8:-4]) == latest:

                    filetype_yyyy = f"{filetype}_{str(file)[-8:-4]}"
                    dfs.setdefault(filetype_yyyy, [])
                    dfs[filetype_yyyy].append(df)

    docs_dir = f"{DATA_DIRECTORY}/docs"

    os.makedirs(docs_dir, exist_ok=True)

    clear_data(docs_dir)

    for _ in tqdm(dfs):

        df = pd.concat(dfs[_], ignore_index=True)
        profile = ProfileReport(
            df,
            title=f"Profile report of {_}, date {date_str}",
            explorative=True,
            # Use accessible palette
            plot={
                "correlation": {"cmap": "viridis", "bad": "#000000"},
                "missing": {"cmap": "viridis", "bad": "#000000"},
            },
        )

        file_html = f"{docs_dir}/{_}.html"

        profile.to_file(file_html)

        with ZipFile(f"{docs_dir}/{date_str}.zip", "a") as zip:
            zip.write(file_html)

        with open(f"{docs_dir}/{_}.json", "w") as file_json:
            json.dump(profile.to_json(), file_json)


if __name__ == "__main__":

    create_eda()
