#!/usr/bin/python3
import os
import psycopg2
import argparse

path = '/data/prcp/cpc_glb_dly_prec'

parser = argparse.ArgumentParser(description='Uploads cpc prcp rasters to db')

parser.add_argument('-f', type=str, metavar='CPC_GLB_DLY_PREC_20160708_float.tif', nargs='?', required=True,
                    help='tif file of CPC')


args = parser.parse_args()

in_file = args.f

try:
    conn = psycopg2.connect("dbname='tlaloc'")
except:
    print("I am unable to connect to the database")
    exit()
cur = conn.cursor()

# remove path
layer = os.path.basename(in_file).replace('.tif','')

# see if file exists in the be
cur.execute("select count(*) from pg_catalog.pg_tables " +
			"where tablename = '{}' and schemaname = 'cpc_glb_dly_prec'".format(layer))
layer_chk = cur.fetchall()[0][0]


if layer_chk == 0:
    tif_file = path + '/' + layer.upper() + '.tif'
    tif_file = tif_file.replace('FLOAT','float')
    os.system("raster2pgsql -C -I -N -999 {tif} ".format(tif=tif_file) +
			  "-d cpc_glb_dly_prec.{db_file} | psql tlaloc".format(db_file=layer))

cur.close()
conn.close()

