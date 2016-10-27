#!/usr/bin/python3
import os
import psycopg2

path = '/data/prcp/cpc_glb_dly_prec'

file_list = os.listdir(path)

# get list of tif
tif_list = []
for i in file_list:
   if i.split('.')[1] == 'tif' and 'latest' not in i:
       tif_list.append(i)


try:
    conn = psycopg2.connect("dbname='fato'")
except:
    print("I am unable to connect to the database")
    exit()
cur = conn.cursor()

cur.execute("select tablename from pg_catalog.pg_tables where schemaname = 'cpc_glb_dly_prec'")
db_list = cur.fetchall()

db_list_cln = []
for i in db_list:
    db_list_cln.append(i[0])

tif_list_db = []
for i in tif_list:
   tif_list_db.append(i.lower().split('.')[0])

upload_list = list(set(tif_list_db) - set(db_list_cln))
if len(upload_list) == 0:
   print("No new images")
   exit(0)

print(len(upload_list))
for i in upload_list:
    tif_file = path + '/' + i.upper() + '.tif'
    tif_file = tif_file.replace('FLOAT','float')
    os.system("raster2pgsql -C -I -N -999 {tif_file} -d cpc_glb_dly_prec.{db_file} | psql fato".format(tif_file=tif_file,db_file=i))
	
cur.close()
conn.close()

