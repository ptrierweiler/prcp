#!/usr/bin/python3
import psycopg2

schema = 'cpc_glb_dly_prec'
table = 'data'

try:
    conn = psycopg2.connect("dbname='tlaloc'")
except:
    print("I am unable to connect to the database")
    exit()
cur = conn.cursor()
conn.autocommit = True


def summerize(layer):
# Find dates in db
    print("Finding dates in {schema}.{table} for {layer}".format(schema=schema, 
	                                                               table=table,
	                                                               layer=layer))
    cur.execute("select distinct date::text from {schema}.{table} \
	               where geolayer = '{layer}'".format(schema=schema, 
	                                                  table=table, layer=layer))
    db_list = cur.fetchall()
    db_list_cln = []
    for i in db_list:
        db_list_cln.append(i[0])
	   
    # image list
    cur.execute("select tablename from pg_catalog.pg_tables \
                 where schemaname = 'cpc_glb_dly_prec' and tablename <> 'data'")
    image_list_db = cur.fetchall()
	
    date_list = []
    for i in image_list_db:
        n = i[0]
        yr = n.split('_')[4][0:4]
        mn = n.split('_')[4][4:6]
        dy = n.split('_')[4][6:8]
        date = "{yr}-{mn}-{dy}".format(yr=yr, mn=mn, dy=dy)
        date_list.append(date)
	
    date_diff_list = list(set(date_list) - set(db_list_cln))
	
	# summerizing files
    for i in date_diff_list:
        print("processing date: " + i)
        image = "cpc_glb_dly_prec.cpc_glb_dly_prec_{date}_float".format(date=i.replace('-',''))
        geo = "wgs84" '.' + layer
        cur.execute('SELECT gid, (stats).count,(stats).mean::numeric(7,3), '+ \
	                  'median::numeric(7,3) FROM (SELECT gid, ' + \
                    'ST_SummaryStats(ST_Clip(rast, {geo}.wkb_geometry)::raster) as stats, '.format(geo=geo) + \
	                  'ST_Quantile(ST_Clip(rast,{geo}.wkb_geometry)::raster,.5) as median '.format(geo=geo) + \
	                  'from {image}, {geo} where '.format(image=image, geo=geo) + \
                    'st_intersects(rast, {geo}.wkb_geometry)) as foo'.format(geo=geo) )
        sum_data = cur.fetchall()
        for row in sum_data:
            gid = row[0]
            cell_cnt = row[1]
            mean = row[2]
            median = row[3]
            date = i
            cur.execute("insert into cpc_glb_dly_prec.data values \
                         (%s, %s, %s, %s, %s, %s)",(gid, cell_cnt, mean, 
                                                    median, date, layer))


summerize('nass_asds')
summerize('ana_bacias')
summerize('brasil_mesoregion')
conn.commit()
cur.close()
conn.close()	