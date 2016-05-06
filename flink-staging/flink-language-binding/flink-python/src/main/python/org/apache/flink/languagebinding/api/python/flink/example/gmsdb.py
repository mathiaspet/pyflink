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

import os.path
import sys

import numpy as np
import psycopg2
import gdal
from gdalconst import GA_ReadOnly

from flink.connection import Collector
from flink.example.ImageWrapper import ImageWrapper, IMAGE_TUPLE
from flink.functions.FilterFunction import FilterFunction
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.io.PythonInputFormat import PythonInputFormat
from flink.plan.Constants import INT
from flink.plan.Environment import get_environment


class GMSDB(PythonInputFormat):
    def __init__(self, dataPath, connection, jobID):
        super(GMSDB, self).__init__()
        gdal.AllRegister()  # TODO: register the ENVI driver only
        self.dataPath = dataPath
        self.connection = connection
        self.jobID = jobID

    def computeSplits(self):
        # get files for predetermining splits
        files = dict()
        conn = psycopg2.connect(**self.connection)
        curs = conn.cursor()
        curs.execute("""SELECT id, filename FROM scenes
                        WHERE id IN (SELECT unnest(sceneids) FROM scenes_jobs WHERE id = %s)""",
                     (self.jobID, ))
        for scene, filename in curs:
            files[os.path.join(self.dataPath, filename)] = scene
        curs.close()
        conn.close()

        self._collector = Collector.Collector(self._connection)
        path = self._iterator.next()
        print("path: ", path)
        for f in files.keys():
            self._collector.collect(f)
        self._collector._close()

    def deliver(self, path, collector):
        # Basic meta data
        """SELECT
                s.datasetid,
                s.acquisitiondate,
                s.entityid,
                s.filename,
                p.proc_level,
                d.image_type,
                sat.name,
                sen.name,
                sub.name
            FROM
                scenes s
            LEFT OUTER JOIN
                scenes_proc p ON p.sceneid = s.id
            LEFT OUTER JOIN
                datasets d ON d.id = s.datasetid
            LEFT OUTER JOIN
                satellites sat ON sat.id = s.satelliteid
            LEFT OUTER JOIN
                sensors sen ON sen.id = s.sensorid
            LEFT OUTER JOIN
                subsystems sub ON sub.id = s.subsystemid
            WHERE
                s.id = %s"""

        print("py: received path:", path)
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
        metaBytes = ImageWrapper._meta_to_bytes(metaData)
        bArr = bytearray(imageData)
        retVal = (metaData['scene id'], metaBytes, bArr)
        collector.collect(retVal)

    def readMetaData(self, path):
        headerPath = path+'.hdr'
        f = open(headerPath, 'r')

        if f.readline().find("ENVI") == -1:
            f.close()
            raise IOError("Not an ENVI header.")

        lines = f.readlines()
        f.close()

        d = {}
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
                        d[key] = txt.strip('{}').strip()
                    else:
                        vals = txt[1:-1].split(',')
                        for j in range(len(vals)):
                            vals[j] = vals[j].strip()
                        d[key] = vals
                else:
                    d[key] = val
            return d
        except:
            raise IOError("Error while reading ENVI file header.")


class Tokenizer(FlatMapFunction):
    def flat_map(self, value, collector):
        print("Just a dumb map function")
        collector.collect(value)


class Adder(GroupReduceFunction):
    def __init__(self):
        super(Adder, self).__init__()
        self.counter = 0

    def reduce(self, iterator, collector):
        for i in iterator:
            self.counter += 1

        collector.collect(self.counter)


class Filter(FilterFunction):
    def __init__(self):
        super(Filter, self).__init__()
        self.count = 0

    def filter(self, value):
        self.count += 1
        print("Image:", value[0], self.count)
        return True


def main():
    try:
        dataPath = sys.argv[1]
    except IndexError:
        dataPath = "/data1/gfz-fe/GeoMultiSens/database/sampledata/"

    connection = {
            "database": "usgscache",
            "user": "gmsdb",
            "password": "gmsdb",
            "host": "localhost",
            "connect_timeout": 3,
            "options": "-c statement_timeout=10000"
        }

    inputFormat = GMSDB(dataPath, connection, 26184107)

    env = get_environment()
    data = env.read_custom(dataPath, ".*?\\.bsq", True, inputFormat, IMAGE_TUPLE)

    result = data \
        .flat_map(Tokenizer(), IMAGE_TUPLE) \
        .filter(Filter()) \
        .group_by(0) \
        .reduce_group(Adder(), INT) \

    result.output()

    env.set_degree_of_parallelism(1)

    env.execute(local=True)


if __name__ == "__main__":
    main()
