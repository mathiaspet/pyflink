################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
from __future__ import print_function
import numpy as np
import gdal
import sys
from gdalconst import GA_ReadOnly
from flink.plan.Constants import BYTES, STRING

from flink.functions.FilterFunction import FilterFunction
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.io.PythonInputFormat import PythonInputFormat, FileInputSplit
from flink.io.PythonOutputFormat import PythonOutputFormat
from flink.plan.Environment import get_environment
from flink.spatial.ImageWrapper import ImageWrapper, TupleToTile, TileToTuple


class Tokenizer(FlatMapFunction):
    def flat_map(self, value, collector):
        print("collecting in Tokenizer")
        sys.stdout.flush()
        collector.collect(value)


class GDALInputFormat(PythonInputFormat):
    def __init__(self, jobID):
        super(GDALInputFormat, self).__init__()
        self.jobID = jobID

    def getFiles(self):
        # get sceneids for jobid:
        # SELECT sceneids FROM scenejobs WHERE id = ?
        # get filenames for sceneids:
        # SELECT filename FROM scenes WHERE id = ?
        # filter bsq?
        files = [
                "file:/opt/gms_sample/227064_000202_BLA_SR.bsq",
                "file:/opt/gms_sample/227064_000321_BLA_SR.bsq"
            ]
        return files

    def createInputSplits(self, minNumSplits, path, collector):
        for f in self.getFiles():
            collector.collect(FileInputSplit(f, 0, 1, ("localhost",)))


    def _userInit(self):
        gdal.AllRegister()  # TODO: register the ENVI driver only

    def deliver(self, split, collector):
        path = split[0]
        ds = gdal.Open(path[5:], GA_ReadOnly)
        if ds is None:
            print('Could not open image', path[5:])
            return
        rows = ds.RasterYSize
        cols = ds.RasterXSize
        bandsize = rows * cols
        bands = ds.RasterCount

        imageData = np.empty(bands * bandsize, dtype=np.int16)
        for j in range(bands):
            band = ds.GetRasterBand(j+1)
            data = np.array(band.ReadAsArray())
            lower = j*bandsize
            upper = (j+1)*bandsize
            imageData[lower:upper] = data.ravel()


        metaData = self.readMetaData(path[5:-4])

        #this is just a hack to cope with the hacky readMetaData-fct (since this is just a throwaway example anyway)
        leftlatlong = metaData["upperleftcornerlatlong"]
        rightlatlong = metaData["lowerrightcornerlatlong"]
        metaData["coordinates"] = [leftlatlong[0], leftlatlong[1], rightlatlong[0], rightlatlong[1]]
        metaData["width"] = metaData["samples"]
        metaData["height"] = metaData["lines"]
        metaData["band"] = "0"
        metaData["xPixelWidth"] = metaData["pixelsize"]
        metaData["yPixelWidth"] = metaData["pixelsize"]

        metaBytes = ImageWrapper._meta_to_bytes(metaData)
        bArr = bytearray(imageData)
        retVal = (metaData['scene id'], metaBytes, bArr)
        print("in pif before collect", type(retVal))
        sys.stdout.flush()
        collector.collect(retVal)

    def readMetaData(self, path):
        headerPath = path+'.hdr'
        f = open(headerPath, 'r')

        if f.readline().find("ENVI") == -1:
            f.close()
            raise IOError("Not an ENVI header.")

        lines = f.readlines()
        f.close()

        dict = {}
        try:
            while lines:
                line = lines.pop(0)
                if '=' not in line or line[0] == ';':
                    continue

                (key, sep, val) = line.partition('=')
                key = key.strip().lower()
                val = val.strip()
                if val and val[0] == '{':
                    txt = val.strip()
                    while txt[-1] != '}':
                        line = lines.pop(0)
                        if line[0] == ';':
                            continue

                        txt += '\n' + line.strip()
                    if key == 'description':
                        dict[key] = txt.strip('{}').strip()
                    else:
                        vals = txt[1:-1].split(',')
                        for j in range(len(vals)):
                            vals[j] = vals[j].strip()
                        dict[key] = vals
                else:
                    dict[key] = val
            return dict
        except:
            raise IOError("Error while reading ENVI file header.")

class GMSOF(PythonOutputFormat):
    def write(self, value):
        print("value", type(value))
        sys.stdout.flush()

class Filter(FilterFunction):
    def __init__(self):
        super(Filter, self).__init__()

    def filter(self, value):
        print(0)
        sys.stdout.flush()
        return False


def main():
    env = get_environment()
    env.set_sendLargeTuples(True)

    inputFormat = GDALInputFormat(26184107)

    data = env.read_custom("/opt/gms_sample/", ".*?\\.bsq", True,
                           inputFormat)

    #result = data \
    #    .flat_map(TupleToTile()) \
    #    .flat_map(Tokenizer()) \
    #    .flat_map(TileToTuple())

    result = data

    result.write_custom(GMSOF("/opt/output"))


    filtered = result.filter(Filter())
    filtered.write_custom(GMSOF("/opt/output"))

    env.set_parallelism(1)

    env.execute(local=True)


if __name__ == "__main__":
    main()
