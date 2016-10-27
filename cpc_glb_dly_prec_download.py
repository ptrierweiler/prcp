#!/usr/bin/python3
import os
import zipfile

def zip_func(zfile, path):
    zipname = path + '/' + zfile
    zip_ref = zipfile.ZipFile(zipname, 'r')
    zip_ref.extractall(path)
    zip_ref.close()

path = '/data/prcp/cpc_glb_dly_prec'
# download using wget should change to python
os.system('wget -r -nc -np -nd -A zip -P {path} http://ftp.cpc.ncep.noaa.gov/GIS/GRADS_GIS/GeoTIFF/GLB_DLY_PREC/daily/'.format(path=path))

# get list of files
file_list = os.listdir(path)

# get list of zip
zip_list = []
for i in file_list:
   if i.split('.')[1] == 'zip' and 'latest' not in i:
       zip_list.append(i)

# unzipping file if not unzipped
for i in zip_list:
   filename = path + '/'+ i
   if os.path.isfile(filename.replace('zip','tif')) == False:
       zip_func(i, path)

# uploading file_list
os.system('/scripts/prcp/cpc_glb_dly_prec_upload.py')
# summerizing 
os.system('/scripts/prcp/cpc_glb_dly_prec_sumerize.py')