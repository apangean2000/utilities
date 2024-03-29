"""
[WIP] Analyse dado_gov_br via ckan api + get site datasets
"""
# TODO URL Fix suggestions - did you mean?
# TODO Maybe add retries on FTP

import csv
import ftplib
import json
import logging
import mimetypes
import os
import re
import socket
import sys
from copy import deepcopy
from http import HTTPStatus
from pathlib import Path
from time import sleep
from typing import Optional, Union

import ftpparser
import requests
from requests.structures import CaseInsensitiveDict
from uri import URI
from yarl import URL  # TODO just use URI package buuut IDNA handling?

logging.basicConfig(stream=sys.stdout, level=logging.ERROR)

URL_BASE: str = "https://dados.gov.br/api/3"
DIRECTORY_DATA = f"data/{sys.argv[0].split('/')[-1]}"
DIRECTORY_MAPPING = f"mapping/{sys.argv[0].split('/')[-1]}"
USER_AGENT: str = "dados.gov.br-ckan-validator"
REQUEST_RETRIES_MAX = 4
REQUEST_TIMEOUT = 15


class Response:
    # Inspired by https://github.com/anyant/rssant/commit/6fcf7fc56c05202d26968cb8b987a242d183eb17
    __slots__ = (
        "_content",
        "_status",
        "_url",
        "_encoding",
        "_etag",
        "_last_modified",
        "_mime_type",
        "_url_redirect",
    )

    def __init__(
        self,
        *,
        content: Optional[bytes] = None,  # HEAD requests
        status: Optional[int] = None,
        url: Optional[str] = None,
        encoding: Optional[str] = None,
        etag: Optional[str] = None,
        last_modified: Optional[str] = None,
        mime_type: Optional[str] = None,
        url_redirect: Optional[str] = None,
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

        try:  # Using some none standard
            status_name = HTTPStatus(self.status).name
        except ValueError:
            status_name = ""

        return (  # TODO full repr
            f"<{name} {self.status} {status_name} url={self.url!r} length={length} "
            f"encoding={self.encoding!r} mimetype={self.mime_type!r}>"
        )

    @property
    def content(self) -> Optional[bytes]:
        return self._content

    @property
    def status(self) -> Optional[int]:
        return self._status

    @property
    def url(self) -> Optional[str]:
        return self._url

    @property
    def encoding(self) -> Optional[str]:
        return self._encoding

    @property
    def etag(self) -> Optional[str]:
        return self._etag

    @property
    def last_modified(self) -> Optional[str]:
        return self._last_modified

    @property
    def mime_type(self) -> Optional[str]:
        return self._mime_type

    @property
    def url_redirect(self) -> Optional[str]:
        return self._url_redirect


class ResponseBuilder:

    __slots__ = (
        "_content",
        "_status",
        "_url",
        "_headers",
        "_use_proxy",
        "_url_redirect",
        "_method",
    )

    def __init__(self, *, use_proxy: bool = False):
        self._content: Optional[bytes] = None  # check
        self._status: Optional[int] = None
        self._url: Optional[str] = None
        self._headers: Optional[CaseInsensitiveDict[str]] = None
        self._use_proxy: Optional[bool] = use_proxy
        self._url_redirect: Optional[str] = None
        self._method: str = ""

    def content(self, value: Optional[bytes]):
        self._content = value

    def status(self, value: Optional[int]):
        self._status = value

    def url(self, value: Optional[str]):
        self._url = value

    def headers(self, headers: Optional[CaseInsensitiveDict[str]]):
        self._headers = headers

    def url_redirect(self, value: Optional[str]):
        self._url_redirect = value

    def method(self, value: str):
        self._method = value

    def build(self) -> Response:
        """
        build _summary_

        :return: _description_
        :rtype: Response
        """

        mime_type = encoding = etag = last_modified = None

        if self._headers:

            content_type_header = self._headers.get("content-type")

            if content_type_header:
                # Perhaps use cgi lib for params
                datas = [_.strip() for _ in content_type_header.split(";")[0:2]]

                mime_type = datas[0]

                if len(datas) > 1:
                    encoding = datas[1].replace(
                        "charset=", ""
                    )  # _parse_content_type_header(content_type_header)

            etag = self._headers.get("etag")
            last_modified = self._headers.get("last-modified")

        content = None

        if self.method and self.method not in ["HEAD"]:
            content = self._content

        # TODO analyze content further,
        # detect_feed_type self._content, mime_type ,
        # detect_content_encoding self._content, http_encoding

        status = self._status  # if self._status is not None else HTTPStatus.OK.value

        return Response(
            content=content,
            status=status,
            url=self._url,
            encoding=encoding,
            etag=etag,
            last_modified=last_modified,
            mime_type=mime_type,
            url_redirect=self._url_redirect,
        )


def csv_append(
    filename: Union[str, Path],
    row_dct: Union[dict, list[dict]],
    fieldnames: list = None,
) -> Union[dict, list[dict]]:
    """
     Append to a csv file

    :param filename: _description_
    :type filename: Union[str, Path]
    :param row_dct: _description_
    :type row_dct: Union[dict, list[dict]]
    :param fieldnames: _description_, defaults to None
    :type fieldnames: list, optional
    :return: _description_
    :rtype: Union[dict, list[dict]]
    """

    if fieldnames is None:
        fieldnames = list(
            row_dct.keys() if isinstance(row_dct, dict) else row_dct[0].keys()
        )

    with open(filename, "a") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        if isinstance(row_dct, dict):
            writer.writerow(row_dct)
        else:
            writer.writerows(row_dct)

    return row_dct


def retry_logger(
    url: str, err: Optional[Exception], status: Optional[int], retries: int
) -> tuple[bool, int]:
    """
    Retry logging and try increment

    :param url: URL
    :type url: str
    :param err: Error
    :type err: Exception
    :param status: response.status_code
    :type status: int
    :param retries: Incrementer_
    :type retries: int
    :return: _description_
    :rtype: tuple[bool,int]
    """

    retries += 1

    if retries == REQUEST_RETRIES_MAX:
        logging.warning(f"Issue with: {url} | Err {err} | Status {str(status)}")
        return (True, retries)

    logging.error(f"Error with: {url} | Err {err} | Status {str(status)}")

    return (False, retries)


def fetch(
    session: requests.Session,
    url: str,
    verb: str,
    timeout: int = REQUEST_TIMEOUT,
    file_output: Union[Path, str] = None,
) -> dict:
    """
    Sync Fetch sa url and write data to file output

    :param session: Session
    :type session: requests.Session
    :param url: URL
    :type url: str
    :param verb: http method
    :type verb: str
    :param timeout: timeout, defaults to REQUEST_TIMEOUT
    :type timeout: int, optional
    :param file_output: output to a file_, defaults to None
    :type file_output: Union[Path,str], optional
    :raises Exception: Raise exceptions for retry
    :return: Return dict
    :rtype: _type_
    """

    retries = 0

    while retries < REQUEST_RETRIES_MAX + 1:

        try:
            response = session.request(
                method=verb, url=url, allow_redirects=True, timeout=timeout
            )

            if response.status_code in (408, 502, 503, 504):

                ret, retries = retry_logger(
                    url=url, err=None, status=response.status_code, retries=retries
                )
                if ret:

                    builder = ResponseBuilder()
                    builder.url(url)
                    builder.status(response.status_code)

                    built = builder.build()

                    out_dct = {
                        attr: getattr(built, attr, None) for attr in built.__slots__
                    }

                    if file_output:
                        csv_append(file_output, out_dct)

                    return out_dct

            elif response.status_code:

                builder = ResponseBuilder()
                builder.url(url)
                builder.status(response.status_code)

                if verb not in ["HEAD"]:
                    builder.content(response.content)

                builder.headers(response.headers)

                if response.history:
                    builder.url(response.history[0].url)
                    builder.url_redirect(response.url)
                else:
                    builder.url(url)

                built = builder.build()

                out_dct = {attr: getattr(built, attr, None) for attr in built.__slots__}

                if file_output:
                    csv_append(file_output, out_dct)

                return out_dct

            else:
                raise Exception
        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.RequestException,
        ) as err:
            ret, retries = retry_logger(url=url, err=err, status=None, retries=retries)
        if ret:

            builder = ResponseBuilder()
            builder.url(url)

            built = builder.build()

            out_dct = {attr: getattr(built, attr, None) for attr in built.__slots__}

            if file_output:
                csv_append(file_output, out_dct)

            return out_dct

        sleep(1 * retries)

    builder = ResponseBuilder()
    builder.url(url)

    built = builder.build()

    out_dct = {attr: getattr(built, attr, None) for attr in built.__slots__}

    return out_dct


def json_process(dct: dict) -> list[dict]:
    """
    Process fields of interest into a dict from ckan response
    #TODO possibly model this as a class
    :param dct: _description_
    :type dct: dict
    :return: _description_
    :rtype: list[dict]
    """

    def dct_package(dct: dict) -> dict:

        measure_dct = {
            _: None
            for _ in [
                "Update",
                "Format",
                "Metadata",
                "License",
                "Data dictionary",
                "Availability",
                "Historic",
                "API",
            ]
        }

        for _ in [
            "license_title",
            "maintainer",
            "maintainer_email",
            "license_id",
            "author",
            "author_email",
        ]:
            measure_dct[_] = dct.get(_)

        for _ in ["id"]:
            measure_dct[f"_package_{_}"] = dct.get(_)

        for _ in data.get("extras", []):
            if dct.get("key", None) == "Frequência de atualização":
                measure_dct["Update"] = dct.get("value")

        return measure_dct

    measure_lst: list[dict] = []

    datas: list = [dct.get("result")]

    for data in datas:

        dct_pack = dct_package(data)

        for _ in data.get("resources", []):

            dct_resource = deepcopy(dct_pack)

            for fieldname in [
                "id",
                "last_modified",
                "format",
                "created",
                "url",
                "state",
                "mimetype",
                "url_type",
                "resource_type",
                "name",
            ]:

                dct_resource[fieldname] = _.get(fieldname)

            measure_lst.append(dct_resource)

    return measure_lst


def ckan_api(session: requests.Session = None) -> requests.Session:
    """
    Call the ckan API

    :param session: With a requests session, defaults to None
    :type session: requests.session, optional
    """

    timeout = 3600

    if session is None:

        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

    os.makedirs(DIRECTORY_DATA, exist_ok=True)

    data_dct = fetch(
        session=session,
        url=f"{URL_BASE}/action/package_list",
        verb="GET",
        timeout=timeout,
    )

    # Write some target fields out, specced in json_process

    file_output = Path(f"{DIRECTORY_DATA}/{sys.argv[0].split('/')[-1]}.csv")
    fieldnames = [
        "Update",
        "Format",
        "Metadata",
        "License",
        "Data dictionary",
        "Availability",
        "Historic",
        "API",
        "license_title",
        "maintainer",
        "maintainer_email",
        "license_id",
        "author",
        "author_email",
        "_package_id",
        "id",
        "last_modified",
        "format",
        "created",
        "url",
        "state",
        "mimetype",
        "url_type",
        "resource_type",
        "name",
    ]
    with open(file_output, mode="w") as fp:
        writer = csv.DictWriter(fp, fieldnames=fieldnames)
        writer.writeheader()

    for idx, _ in enumerate(json.loads(data_dct["_content"])["result"]):

        url = f"{URL_BASE}/action/package_show?id={_}"
        datas = fetch(
            session=session,
            url=url,
            verb="GET",
            timeout=timeout,
        )

        if datas["_content"]:
            # API contract is pretty stable, hence no error checking
            csv_append(
                file_output, json_process(json.loads(datas["_content"])), fieldnames
            )

    logging.info(f"Rows extracted {idx}")

    return session


def ckan_url(session: requests.Session = None) -> requests.Session:
    """
    Get urls from a list if available, else call api to generate one
    Then loop through checking status, encoding etc

    :param session: Optional session, defaults to None
    :type session: requests.session, optional
    """

    if session is None:

        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

    file_url = Path(f"{DIRECTORY_DATA}/{sys.argv[0].split('/')[-1]}.csv")
    if file_url.is_file() is False:
        ckan_api()

    os.makedirs(f"_{DIRECTORY_DATA}", exist_ok=True)

    file_output = Path(f"_{DIRECTORY_DATA}/{sys.argv[0].split('/')[-1]}.csv")
    with open(file_output, mode="w") as fp:
        writer = csv.DictWriter(fp, fieldnames=Response.__slots__)
        writer.writeheader()

    urls_tmp: list[str] = []
    urls: list[str] = []

    with open(file_url) as f:
        _ = list({_["url"] for _ in list(csv.DictReader(f))})
        if _:
            urls_tmp = _.sort()

    for _ in urls_tmp:  # Prescreen for malformed up front

        raisenwrite = False

        try:
            url = URL(_)
            if len(url.host) > 64 or url.port is None:
                raisenwrite = True
            else:
                urls.append(_)
        except (ValueError, TypeError, UnicodeError):
            raisenwrite = True

        if raisenwrite:
            logging.error(f"Issue with {_}")
            builder = ResponseBuilder()
            builder.url(url)
            built = builder.build()
            csv_append(
                file_output,
                {attr: getattr(built, attr, None) for attr in built.__slots__},
            )

    for url in urls:
        fetch(
            session=session,
            url=url,
            verb="HEAD",
            file_output=file_output,
        )

    return session


def uris_log(uris: list[URI], file_output: Path) -> None:
    # TODO add status code in here perhaps
    # For additional diagnostics
    """
    Log a list of URI classes

    :param uris: list of URI classes
    :type uris: list[URI]
    :param file_output: file to send to
    :type file_output: str
    """

    for _ in uris:
        logging.error(f"Issue with {_.uri}")
        builder = ResponseBuilder()
        builder.url(_.uri)
        built = builder.build()
        csv_append(
            file_output,
            {attr: getattr(built, attr, None) for attr in built.__slots__},
        )


def ckan_uri_scheme(session: requests.Session = None) -> bool:
    """
    Get endpoints from any scheme, should be deprecated whne ftp removed
    # TODO return FTP status codes
    :param session: _description_, defaults to None
    :type session: requests.Session, optional
    :raises Exception: _description_
    :raises Exception: _description_
    :raises Exception: _description_
    :raises Exception: _description_
    :return: _description_
    :rtype: bool
    """

    file_input = Path(f"{DIRECTORY_DATA}/{sys.argv[0].split('/')[-1]}.csv")

    file_output = Path(f"_{DIRECTORY_DATA}/{sys.argv[0].split('/')[-1]}.csv")

    os.makedirs(DIRECTORY_DATA, exist_ok=True)

    with open(file_output, mode="w") as fp:
        writer = csv.DictWriter(fp, fieldnames=Response.__slots__)
        writer.writeheader()

    urls: list = []
    uris: dict = {}

    with open(file_input) as f:
        # erroneously described as urls but stores uri's
        urls = list({_["url"] for _ in list(csv.DictReader(f))})

    for _ in urls:  # Prescreen for malformed up front

        raisenwrite = False

        try:
            # https://github.com/marrow/uri
            uri_posit = URI(_)

        except ValueError:

            raisenwrite = True

        if (uri_posit.scheme is None or uri_posit.hostname is None) or (
            len(uri_posit.hostname) > 64
        ):

            raisenwrite = True

        else:
            # Use this for ftp dir, improve dns
            sans_qry_fragment = "/".join(uri_posit.uri.split("/")[:-1])

            try:
                uri_posit.scheme.name
            except AttributeError:
                raisenwrite = True

            if raisenwrite is False:

                if sans_qry_fragment not in uris:
                    uris[sans_qry_fragment] = []

                uris[sans_qry_fragment].append(uri_posit)

        if raisenwrite:
            logging.error(f"Issue with {_}")
            builder = ResponseBuilder()
            builder.url(_)
            built = builder.build()
            csv_append(
                file_output,
                {attr: getattr(built, attr, None) for attr in built.__slots__},
            )

    if session is None:
        # Could check read of file for http / https,
        # but just setup here
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})

    # We use this to infer encoding / type in ftp , again could check we have some requests L712
    mimetypes.init()

    for sans_qry_fragment, uri_by_paths in uris.items():

        uri: URI = uri_by_paths[0]

        scheme: str = uri.scheme.name.strip()

        if scheme in ["ftp"]:

            if (
                re.search("ftp://ftp.hfa.mil.br", str(uri_by_paths[0].uri), re.DOTALL)
                is None
            ):
                continue

            # We just get first domain / path and search for files
            hasconnect = False

            try:
                ftp = ftplib.FTP(
                    host=uri.host,
                    user=uri.username if uri.username else None,
                    passwd=uri.password if uri.password else None,
                    # https://bugs.python.org/issue30956
                    timeout=REQUEST_TIMEOUT,
                )
                ftp.login()
                hasconnect = True

            except ftplib.all_errors as e:

                if isinstance(e, socket.timeout):  # Not sure this is the best way

                    uris_log(uri_by_paths, file_output)

                else:
                    raise

            if hasconnect is True:  # TODO handle this better from above

                files_dct: dict = {}
                dir_list: list[str] = []

                try:
                    # Capture transient errors here
                    if str(uri.path.parent) != "/":
                        ftp.cwd(uri.path.parent)

                    ftp.retrlines("LIST", dir_list.append)  # 'MLSD'?

                except ftplib.all_errors:  # TODO to broad here
                    uris_log(uri_by_paths, file_output)

                ftp.quit()

                parser = ftpparser.FTPParser()

                # name, size, timestamp, isdirectory, downloadable, islink, permissions
                files_dct = {
                    f"{sans_qry_fragment}/{_[0]}": {
                        "name": _[0],
                        "size": _[1],
                        "timestamp": _[2],
                        "url_redirect": _[5],
                    }
                    for _ in parser.parse(dir_list)
                    if _[3] == 0 and _[4] == 1
                }

                # Guess the file type and encoding, could download and inspect
                # Buut FTP likely to be deprecated in future
                for _ in files_dct:
                    (
                        files_dct[_]["mime_type"],
                        files_dct[_]["encoding"],
                    ) = mimetypes.guess_type(_)

                # Just check file exists in expected location

                for _ in uri_by_paths:

                    if _.uri in files_dct:

                        file = files_dct[_.uri]
                        builder = ResponseBuilder()
                        builder.url(_.uri)
                        builder.status(200)
                        builder.headers(
                            CaseInsensitiveDict(
                                {
                                    "content-type": f"{file.get('mime_type','')};{file.get('encoding','')}",
                                    "last_modified": file.get("timestamp", ""),
                                }
                            )
                        )

                        if files_dct[_.uri]["url_redirect"]:
                            builder.url_redirect(files_dct[_.uri]["url_redirect"])

                        built = builder.build()

                        out_dct = {
                            attr: getattr(built, attr, None) for attr in built.__slots__
                        }

                        if file_output:
                            csv_append(file_output, out_dct)
                    else:
                        uris_log([_], file_output)

        elif scheme in ["http", "https"]:
            for url in uri_by_paths:
                fetch(
                    session=session,
                    url=url.uri,
                    verb="HEAD",
                    file_output=file_output,
                )
        else:
            uris_log(uri_by_paths, file_output)

    return True


if __name__ == "__main__":
    ckan_uri_scheme()
