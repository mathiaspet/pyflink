# script to get pixel values at a set of coordinates
# by reading in one pixel at a time
# Took 0.47 seconds on my machine
import os, sys, time, gdal
import numpy as np
from gdalconst import *
import spectral.io.envi as envi
import __builtin__ as builtins
# start timing
startTime = time.time()
# coordinates to get pixel values for

# set directory
os.chdir(r'/opt/gms_sample')
# register all of the drivers
gdal.AllRegister()
# open the image
ds = gdal.Open('227064_000321_BLA_SR.bsq', GA_ReadOnly)
if ds is None:
    print 'Could not open image'
    sys.exit(1)
else:
    print 'opened image successfully'

rows = ds.RasterYSize
cols = ds.RasterXSize
bandsize = rows * cols
bands = ds.RasterCount

print 'Got ',cols,' columns and ',rows,' rows. The image contains ',bands,' bands.'

imageData = np.empty(bands * bandsize)
for j in range(bands):
    band = ds.GetRasterBand(j+1)
    data = np.array(band.ReadAsArray())
    lower = j*bandsize
    upper = (j+1)*bandsize
    imageData[lower:upper] = data.ravel()
    print 'read band',j+1

print imageData.shape

print ds.GetGeoTransform()

f = builtins.open('227064_000321_BLA_SR.hdr', 'r')

if f.readline().find("ENVI") == -1:
    f.close()
    raise IOError("Not an ENVI header.")

lines = f.readlines()
f.close()

dict = {}
try:
    while lines:
        line = lines.pop(0)
        if line.find('=') == -1: continue
        if line[0] == ';': continue

        (key, sep, val) = line.partition('=')
        key = key.strip().lower()
        val = val.strip()
        if val and val[0] == '{':
            str = val.strip()
            while str[-1] != '}':
                line = lines.pop(0)
                if line[0] == ';': continue

                str += '\n' + line.strip()
            if key == 'description':
                dict[key] = str.strip('{}').strip()
            else:
                vals = str[1:-1].split(',')
                for j in range(len(vals)):
                    vals[j] = vals[j].strip()
                dict[key] = vals
        else:
            dict[key] = val
    print dict
except:
    raise IOError("Error while reading ENVI file header.")