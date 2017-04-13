#!/usr/bin/python3
import os
import zipfile
import datetime
import psycopg2
from bs4 import BeautifulSoup
import requests

url = 'http://ftp.cpc.ncep.noaa.gov/GIS/GRADS_GIS/GeoTIFF/GLB_DLY_PREC/daily/'
soup = BeautifulSoup(requests.get(url).text, "html5lib")

try:
    conn = psycopg2.connect("dbname='tlaloc'")
except:
    print("I am unable to connect to the database")
    exit()

cur = conn.cursor()


def zip_func(zfile, path):
    zipname = path + '/' + zfile
    zip_ref = zipfile.ZipFile(zipname, 'r')
    zip_ref.extractall(path)
    zip_ref.close()


cur.execute("select tablename from pg_catalog.pg_tables " +
            "where schemaname = 'cpc_glb_dly_prec' and " +
            "tablename <> 'data'")
db_list = cur.fetchall()
    
    
href_dict = {}
for a in soup.find_all('a'):
    href = a['href']
    if href[-3:] == 'zip' and 'latest' not in href:
        fdate = datetime.datetime.strptime(href.split('_')[-2], '%Y%m%d').date()
        tmp_dict = {fdate: href}
        href_dict.update(tmp_dict)


db_list_date = []
for i in db_list:
    if i[0] != 'data':
        db_date = datetime.datetime.strptime(i[0].split('_')[-2], '%Y%m%d').date()
        db_list_date.append(db_date)

href_list = list(href_dict.keys())

diff_list = list(set(href_list) - set(db_list_date))

for i in diff_list:
    name = href_dict[i]
    tif_name = name.replace('.zip', '.tif')
    d_link = url + name
    res = requests.get(d_link)
    name_file = open('/data/prcp/cpc_glb_dly_prec/' + name, 'wb')
    for chunk in res.iter_content(100000):
        name_file.write(chunk)
    zip_func(name, '/data/prcp/cpc_glb_dly_prec/')
    tif_file = '/data/prcp/cpc_glb_dly_prec/' + tif_name
    # uploading file_list create input
    os.system('/scripts/prcp/cpc_glb_dly_prec_upload.py -f ' + tif_file)
    # summerizing create input
    os.system('/scripts/prcp/cpc_glb_dly_prec_sumerize.py')
    # delete files
    os.remove(tif_file)
    os.remove('/data/prcp/cpc_glb_dly_prec/' + name)