"""
Analyse dado_gov_br site datasets

#TODO test aoidns
#TODO cacert https://github.com/anyant/rssant/search?q=cert
#TODO UnicodeError: encoding with 'idna' codec failed (UnicodeError: label empty or too long)
Metrics
- Update 
Checks if the dataset is up-to-date, based on the frequency update defined
No 
Yes
- Format 
Checks if data is published in open format (machine-readable structured formats) 
No (PDF, Excel)
Yes (CSV, JSON, RDF, XML, others tbd)
- Metadata
Checks if metadata indicated as mandatory is informed.
No 
Yes
- License **done
Checks if the dataset is associated with a free license (define set of licenses that can be used)
No
Yes
- Data dictionary
Checks for registered data dictionary for a dataset
No
Yes
- Availability
Checks if resource links are broken. 
No 
Yes
- Historic
Evaluates, in the metadata, whether information about creation date, update dates, and last update is provided
No 
Yes
- API
Checks whether data is made available for access and consultation through APIs. See if any of the registered resources is an API. 
No
Yes
"""

import asyncio
import aiohttp
import logging
import sys
import csv
import os
from typing import Union, Optional
from pathlib import Path
from copy import deepcopy
import json
import re
import multidict
import requests
from yarl import URL
from http import HTTPStatus
from time import sleep


logging.basicConfig(level=logging.ERROR)

URL_BASE:str = 'https://dados.gov.br/api/3'
DIRECTORY_DATA = f"data/{sys.argv[0].split('/')[-1]}"
DIRECTORY_MAPPING = f"mapping/{sys.argv[0].split('/')[-1]}"
USER_AGENT:str = 'dados.gov.br-ckan-validator'
REQUEST_RETRIES_MAX = 4
REQUEST_TIMEOUT = 15


def clear_data(directory: str) -> None:
    """
    Clear data directory, ideally in a helper file
    """

    directory_p = Path(directory)

    if os.path.exists(directory_p):
        for item in directory_p.iterdir():
            if item.is_dir():
                clear_data(str(item))
            else:
                item.unlink()


def write_csv(filename:str, row_dct:dict) -> dict:

    #filename = f"{DATA_DIRECTORY}/_http_{sys.argv[0].split('/')[-1]}.csv"

    with open(filename, 'a') as fp:
        writer = csv.DictWriter(fp,fieldnames=list(row_dct.keys()))
        writer.writerow(row_dct)

    return row_dct

class Response:
    #Inspired by https://github.com/anyant/rssant/blob/master/LICENSE
    __slots__ = (
        '_content',
        '_status',
        '_url',
        '_encoding',
        '_etag',
        '_last_modified',
        '_mime_type',
        '_url_redirect',
    )

    def __init__(
        self, *,
        content: bytes = None,
        status: Optional[int] = None,
        url: str = None,
        encoding: str = None,
        etag: str = None,
        last_modified: str = None,
        mime_type: str = None,
        url_redirect: str = None
    ):
        self._content = content
        self._status = int(status) if status else None
        self._url = url
        self._encoding = encoding
        self._etag = etag
        self._last_modified = last_modified
        self._mime_type = mime_type
        self._url_redirect = url_redirect

    def __repr__(self):

        name = type(self).__name__
        length = len(self._content) if self._content else 0

        try: # Using some none standard
            status_name = HTTPStatus(self.status).name
        except:
            status_name = ''

        return ( # TODO full repr
            f'<{name} {self.status} {status_name} url={self.url!r} length={length} '
            f'encoding={self.encoding!r} mimetype={self.mime_type!r}>'
        )

    @property
    def content(self) -> bytes:
        return self._content

    @property
    def status(self) -> int:
        return self._status

    @property
    def url(self) -> str:
        return self._url

    @property
    def encoding(self) -> str:
        return self._encoding

    @property
    def etag(self) -> str:
        return self._etag

    @property
    def last_modified(self) -> str:
        return self._last_modified

    @property
    def mime_type(self) -> str:
        return self._mime_type

    @property
    def url_redirect(self) -> str:
        return self._url_redirect


class ResponseBuilder:

    __slots__ = (
        '_content',
        '_status',
        '_url',
        '_headers',
        '_use_proxy',
        '_url_redirect',
        '_method'
    )

    def __init__(self, *, use_proxy=False):
        self._content:bytes = None # check
        self._status: Optional[int] = None
        self._url:str = None
        self._headers:multidict = None
        self._use_proxy:bool = use_proxy
        self._url_redirect:str = None

    def content(self, value: bytes):
        self._content = value

    def status(self, value: str):
        self._status = value

    def url(self, value: str):
        self._url = value

    def headers(self, headers: dict):
        self._headers = headers

    def url_redirect(self, value: str):
        self._url_redirect = value

    def method(self, value: str):
        self._method = value

    def build(self) -> Response:

        mime_type = encoding = etag = last_modified =None

        if self._headers:
           
            content_type_header = self._headers.get('content-type')

            if content_type_header:
                #mime_type, encoding = _parse_content_type_header(content_type_header)

                datas = [_.strip() for _ in content_type_header.split(';')[0:2]]

                mime_type = datas[0] 

                if len(datas) > 1:
                    encoding = datas[1].replace('charset=','') # _parse_content_type_header(content_type_header)

            etag = self._headers.get("etag")
            last_modified = self._headers.get("last-modified")

        content = None

        if self.method and self.method not in ['HEAD']:
            content = self._content

        #if self._content:
        #    feed_type = detect_feed_type(self._content, mime_type)
        #    encoding = detect_content_encoding(self._content, http_encoding)

        status = self._status #if self._status is not None else HTTPStatus.OK.value

        return Response(
            content= content,
            status= status,
            url= self._url,
            encoding= encoding,
            etag= etag,
            last_modified= last_modified,
            mime_type= mime_type,
            url_redirect= self._url_redirect
        )


def retry_logger(url:str, err:Exception, status:int, retries:int) -> tuple[bool,int]:
    
    retries += 1

    if retries==REQUEST_RETRIES_MAX: 
        logging.warning(f"Issue with: {url} | Err {err} | Status {str(status)}")
        return (True,retries)

    logging.error(f"Error with: {url} | Err {err} | Status {str(status)}")

    return (False,retries)


def fetch(session:requests.Session, url:str, verb:str, timeout:int=REQUEST_TIMEOUT, file_output:Union[Path,str]=None) -> None:

    retries = 0

    while retries < REQUEST_RETRIES_MAX+1:

        try:
            response = session.request(method=verb,url=url,allow_redirects=True,timeout=timeout)

            if response.status_code in (408, 502, 503 , 504):

                ret, retries = retry_logger(url=url, 
                                            err=None, 
                                            status=response.status_code, 
                                            retries=retries)
                if ret:

                    builder = ResponseBuilder()
                    builder.url(url)
                    builder.status(response.status_code)

                    built = builder.build()

                    out_dct = {attr: getattr(built, attr, None) for attr in built.__slots__}

                    if file_output:
                        write_csv(file_output,out_dct)

                    return out_dct

            elif response.status_code:
                                    
                builder = ResponseBuilder()
                builder.url(url)
                builder.status(response.status_code)

                if verb not in ['HEAD']:
                    builder.content(response.content)

                builder.headers(response.headers)

                if response.history:
                    builder.url(response.history[0].url)
                    builder.url_redirect(response.url)
                else:
                    builder.url(url)

                # TODO add history to get the original undirected URL
                built = builder.build()

                out_dct = {attr: getattr(built, attr, None) for attr in built.__slots__}

                if file_output:
                    write_csv(file_output,out_dct)

                return out_dct

            else:
                raise Exception
        except (requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
                requests.exceptions.RequestException) as err:
                ret, retries = retry_logger(url=url, 
                                            err=err, 
                                            status=None, 
                                            retries=retries)
        if ret:

            builder = ResponseBuilder()
            builder.url(url)

            built = builder.build()

            out_dct = {attr: getattr(built, attr, None) for attr in built.__slots__}

            if file_output:
                write_csv(file_output,out_dct)

            return out_dct
        
        sleep(1*retries)
            

def json_process(dct:Union[dict,list]) -> dict: 

    def dct_package(dct:dict) ->dict:

        measure_dct =  {_:None for _ in ['Update','Format','Metadata','License','Data dictionary','Availability','Historic','API']}
        
        for _ in ['license_title','maintainer','maintainer_email','license_id','author','author_email']:
            measure_dct[_] =  dct.get(_)

        for _ in ['id']:
            measure_dct[f'_package_{_}'] =  dct.get(_)

        for _  in data.get('extras',[]):
            if dct.get('key',None) == 'Frequência de atualização':
                measure_dct['Update'] =  dct.get('value')

        return measure_dct

    measure_lst:list = []

    if datas := [dct.get('result')]:
        #TODO funky may get array as tasks complete it appears
        pass
    else:
        datas = [_['result'] for _ in dct]

    for data in datas:

        dct_pack = dct_package(data)

        for _ in data.get('resources',[]):
            
            dct_resource = deepcopy(dct_pack)

            for fieldname in ['id','last_modified','format','created','url','state','mimetype','url_type','resource_type','name']:

                dct_resource[fieldname] = _.get(fieldname)

            measure_lst.append(dct_resource)

    return measure_lst


def ckan_api(session:requests.session=None) -> None: 

    timeout=3600

    if session is None:

        session = requests.Session()
        session.headers.update({'User-Agent':USER_AGENT})
        
    clear_data(DIRECTORY_DATA)

    os.makedirs(DIRECTORY_DATA, exist_ok=True)

    data_dct = fetch(session=session, url = f'{URL_BASE}/action/package_list', verb="GET", timeout=timeout)

    file_output = Path(f"{DIRECTORY_DATA}/{sys.argv[0].split('/')[-1]}.csv")

    for idx,_ in  enumerate(json.loads(data_dct['_content'])['result']):
        url = f'{URL_BASE}/action/package_show?id={_}'
        fetch(session=session,url = url, verb="GET",timeout=timeout, file_output=file_output) 

    logging.info(f'Rows extracted {idx}')


def ckan_url(session:requests.session=None) -> None: 

    if session is None:

        session = requests.Session()
        session.headers.update({'User-Agent':USER_AGENT})
        

    file_url = Path(f"{DIRECTORY_DATA}/{sys.argv[0].split('/')[-1]}.csv")
    if file_url.is_file() == False:
        ckan_api()

    os.makedirs(f"_{DIRECTORY_DATA}", exist_ok=True)

    file_output = Path(f"_{DIRECTORY_DATA}/{sys.argv[0].split('/')[-1]}.csv")
    with open(file_output, mode="w") as fp:
        writer = csv.DictWriter(fp, fieldnames = Response.__slots__)
        writer.writeheader()

    urls_tmp = urls = []

    with open(file_url,"r") as f:
        urls_tmp = list(set([_['url'] for _ in list(csv.DictReader(f))]))

    for _ in urls_tmp: # Check for malformed up front

        raisenwrite = False
        try:
            url = URL(_)
            if len(url.host) > 64 or url.port is None:
                raisenwrite = True
            else:
                urls.append(_)
        except:
            raisenwrite = True

        if raisenwrite:
            logging.error(f'Issue with {_}')
            builder = ResponseBuilder()
            builder.url(url)
            built = builder.build()
            write_csv(file_output,{attr: getattr(built, attr, None) for attr in built.__slots__})

    for url in urls:
        fetch(session=session, url=url, verb='HEAD', file_output=file_output,)

if __name__ == '__main__':
    ckan_url()
    #ckan_api()