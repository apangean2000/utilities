"""
[WIP][TODO] Find common technology signatures in hrefs 
"""

import re

import requests

SITES = """
demo.ckan.org
data.balikpapan.go.id
data.bart.gov
dadosabertos.bcb.gov.br;opendata.bcb.gov.br
datenregister.berlin.de/*
data.birmingham.gov.uk
data.birminghamal.gov
dadosabertos.bndes.gov.br
datosabiertos.bogota.gov.co
data.boston.gov
kod.brno.cz
data.buenosaires.gob.ar
ckan.cabi.org
opendata.cabq.gov
datos.caf.com
datos.cali.gov.co
datosestadistica.cba.gov.ar;datosgestionabierta.cba.gov.ar
dataproducts.cbrands.com
claircitydata.cbs.nl
data.cdc.gov.tw
datochq.chequeado.com;dato.chequeado.com
dataportal.cps.chula.ac.th
opendata.city-adm.lviv.ua
data.cityofdenton.com
denton.civicdashboards.com;catalog.civicdashboards.com;bernco.civicdashboards.com
demo.ckan.org;beta.ckan.org;master.ckan.org
data.corkcity.ie
bmckan.cpami.gov.tw
datos.cultura.gob.ar
data.kalamazoocity.org
transparenz.karlsruhe.de
data.kcg.gov.tw
dataplatform.knmi.nl;catalog.dataplatform.knmi.nl
data.pref.kyoto.lg.jp
scidm.nchc.org.tw;ipgod.nchc.org.tw
opend-portal.nectec.or.th
resiliencedashboard.nfwf.org;resiliencedata.nfwf.org
data.nhm.ac.uk
pe.181.209.63.227.nip.io;cg.181.209.63.227.nip.io
data.num.edu.mn
data.ok.gov
test.data.ontario.ca;data.ontario.ca;stage.data.ontario.ca
opendataphilly.org
buildingamerica.openei.org
data.city.osaka.lg.jp
data.tainan.gov.tw
datos.techo.org
data.tegalkab.go.id
data.tn.gov/*
data-test.toll.no;data.toll.no/*
dadosabertos.tse.jus.br
"""


def longest_common_substring(s1: str, s2: str) -> str:
    """
    longest_common_substring _summary_

    :param s1: String A
    :type s1: str
    :param s2: String B
    :type s2: str
    :return: Longest String
    :rtype: _type_
    """
    m = [[0] * (1 + len(s2)) for _ in range(1 + len(s1))]

    longest, x_longest = 0, 0

    for x in range(1, 1 + len(s1)):
        for y in range(1, 1 + len(s2)):
            if s1[x - 1] == s2[y - 1]:
                m[x][y] = m[x - 1][y - 1] + 1
                if m[x][y] > longest:
                    longest = m[x][y]
                    x_longest = x
            else:
                m[x][y] = 0

    return s1[x_longest - longest : x_longest]


urls = [_.split(";") for _ in SITES.splitlines() if _ != ""]
urls = [f"http://{_}" for sublist in urls for _ in sublist]

href_re = re.compile("href=(?:\"|')?([^\"|']+)", flags=re.M)

try:
    common_str = longest_common_substring(
        " ".join(re.findall(href_re, requests.get(urls[0], verify=False).text)),
        " ".join(re.findall(href_re, requests.get(urls[1], verify=False).text)),
    )
except:
    print("Something bad happened")


urls_nocommon = []
urls_errs = []

for url in urls[2:]:
    for url in url.split(";"):
        try:
            if except_url := re.search(
                common_str, requests.get(url, verify=False).text, re.M
            ):
                urls_nocommon.append(url)
        except:
            urls_errs.append(url)

print("common_str", common_str)
print("urls_nocommon", urls_nocommon)
print("urls_errs", urls_errs)
