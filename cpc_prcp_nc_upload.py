#!/usr/bin/python3
import os
import zipfile
from datetime import datetime, timedelta
import psycopg2
import argparse
import json
import urllib.request

parser = argparse.ArgumentParser(description='Download and ingest CPC reanalysis temperture')

parser.add_argument('-y', type=str, metavar='2016', nargs='?', required=True,
                    help='Year of data')

args = parser.parse_args()
year = args.y

# load config file
with open('/scripts/config.json') as j:
    config = json.load(j)

path = config['cpc_path']
dbname = config['dbname']
link = "ftp://ftp.cdc.noaa.gov/Datasets/cpc_global_precip/precip.{}.nc".format(year)

# file test overwriting if exists
cpc_file = os.path.join(path,"air.sig995.{}.nc".format(year))
if os.path.isfile(cpc_file) is True:
   os.remove(cpc_file)

print("Downloading", link)
urllib.request.urlretrieve(link, cpc_file)

 # creating list of bands
band_file = '/scripts/prcp/band.txt'
if os.path.isfile(band_file) is True:
    print("Removing old band file")    
    os.remove(band_file)


os.system('gdalinfo -nomd NETCDF:"{}":precip > {}'.format(cpc_file, band_file))

with open(band_file, 'r', encoding='utf-8') as f:
    bands = f.readlines()

# finding max band
max_band = int(bands[-3].split()[1])
print(max_band)

try:
    conn = psycopg2.connect("dbname='{}'".format(dbname))
except:
    print("I am unable to connect to the database")
    exit()

conn.autocommit = True
cur = conn.cursor()


for i in range(1,max_band+1):
    tif_name = "cpc_prcp_{}{}".format(str(year), str(int(i)).zfill(3))
    name = os.path.join(path,tif_name)
    print(name)
    db_date = datetime.strptime(tif_name.split('_')[2],'%Y%j').strftime('%Y%m%d')
    table = "cpc_temp_{}".format(db_date)
    cur.execute("select count(*) from pg_catalog.pg_tables where "\
                "schemaname = 'cpc_prcp' and tablename = '{}'".format(table))
    tab_out = cur.fetchall()[0][0]    
    if tab_out == 0:
        print("Extracting " + name)
        os.system('gdal_translate -b {} NETCDF:"{}":precip {}.tif'.format(i, cpc_file, name))
        print("Splitting " + name)
        os.system('gdal_translate -srcwin 0 0 72 73 -a_ullr 0 90 180 -90 {out}.tif {out}_east.tif'.format(out=name))
        os.system('gdal_translate -srcwin 72 0 72 73 -a_ullr -180 90 0 -90 {out}.tif {out}_west.tif'.format(out=name))
        print("Merging " + name.replace('.tif','_fix.tif'))
        os.system('gdal_merge.py -o {out}_fix.tif {out}_east.tif {out}_west.tif'.format(out=name))
        print(db_date)
        os.system("gdalwarp -t_srs WGS84 {out}_fix.tif {out}_wgs84.tif".format(out=name))
        os.system("raster2pgsql -I {out}_wgs84.tif -d cpc_prcp.{tab} | psql {db}".format(out=name, tab=table, db=dbname))
        os.remove("{}_east.tif".format(name))
        os.remove("{}_west.tif".format(name))
        os.remove("{}.tif".format(name))
        os.remove("{}_wgs84.tif".format(name))
        os.remove("{}_fix.tif".format(name))
        print("{}.tif".format(name))
        # resample down .25
        cur.execute("update cpc_prcp.{} set rast=ST_Rescale(rast, .25, -.25)".format(table))
    else:
        print(table + " exists in the db")
