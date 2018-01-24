#!/usr/bin/python3
import os
import zipfile
from datetime import datetime, timedelta
import psycopg2
import argparse
import json
import urllib.request

schema = 'cpc_prcp'
table = 'data'

parser = argparse.ArgumentParser(description='Download and ingest CPC precp')

parser.add_argument('-y', type=str, metavar='2016', nargs='?', required=True,
                    help='Year of data')

parser.add_argument('-g', type=str, metavar='nass_asds', nargs='?', required=True,
                    help='geo layer')

args = parser.parse_args()
year = args.y
in_geo = args.g

# load config file
with open('/scripts/config.json') as j:
    config = json.load(j)

dbname = config['dbname']

try:
    conn = psycopg2.connect("dbname='{}'".format(dbname))
except:
    print("I am unable to connect to the database")
    exit()

conn.autocommit = True
cur = conn.cursor()


def summerize(layer):
    # Find dates in db
    print("Finding dates in {}.{} for {}".format(schema, table, layer))
    cur.execute("select distinct date::text from {}.{} \
                   where geolayer = '{}'".format(schema, table, layer))
    db_list = cur.fetchall()
    db_list_cln = []
    for i in db_list:
        db_list_cln.append(i[0])
       
    # image list
    cur.execute("select tablename from pg_catalog.pg_tables \
                 where schemaname = 'cpc_prcp' and tablename <> 'data'")
    image_list_db = cur.fetchall()
    date_list = []
    for i in image_list_db:
        n = i[0]
        py_date = datetime.strptime(n.split('_')[-1], '%Y%m%d')
        date = datetime.strftime(py_date, '%Y-%m-%d')
        date_list.append(date)
    
    date_diff_list = list(set(date_list) - set(db_list_cln))
    date_diff_list.sort()
    # summerizing files
    for i in date_diff_list:
        print("processing date: " + i)
        
        image = "{}.cpc_prcp_{}".format(schema, i.replace('-',''))
        geo = "wgs84" '.' + layer
        cur.execute('SELECT gid, (stats).count,(stats).mean::numeric(8,3), '+ \
                    'median::numeric(7,3) FROM (SELECT gid, ' + \
                    'ST_SummaryStats(ST_Clip(rast, {}.wkb_geometry)::raster) as stats, '.format(geo) + \
                    'ST_Quantile(ST_Clip(rast,{}.wkb_geometry)::raster,.5) as median '.format(geo) + \
                    'from {}, {} where '.format(image, geo) + \
                    'st_intersects(rast, {}.wkb_geometry)) as foo'.format(geo))
        sum_data = cur.fetchall()
        for row in sum_data:
            gid = row[0]
            cell_cnt = row[1]
            mean = row[2]
            median = row[3]
            date = i
            cur.execute("insert into cpc_prcp.data values \
                         (%s, %s, %s, %s, %s, %s)",(gid, cell_cnt, mean, 
                                                    median, date, layer))

summerize(in_geo)
cur.close()
conn.close()
