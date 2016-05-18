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
import pickle

import psycopg2
import psycopg2.extras
import gdal

from flink.connection import Collector
from flink.example.ImageWrapper import IMAGE_TUPLE
from flink.functions.FilterFunction import FilterFunction
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.io.PythonInputFormat import PythonInputFormat
from flink.plan.Constants import INT
from flink.plan.Environment import get_environment

from flink.example.gmsdb.lvl0a import get_metadata
from flink.example.gmsdb.misc import get_path, get_scenes


class GMSDB(PythonInputFormat):
    def __init__(self, data_path, connection, job_id):
        super(GMSDB, self).__init__()
        gdal.AllRegister()  # TODO: register the ENVI driver only
        self.job = {
                'id': job_id,
                'data_path': data_path,
                'connection': connection,
                'skip_pan': False,
                'skip_thermal': False
            }

    def get_path(self, scene):
        conn = psycopg2.connect(**self.job['connection'])
        curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        curs.execute(
                """SELECT
                        s.filename,
                        sat.name AS satellite,
                        sen.name AS sensor
                    FROM
                        scenes s
                    LEFT OUTER JOIN
                        satellites sat ON sat.id = s.satelliteid
                    LEFT OUTER JOIN
                        sensors sen ON sen.id = s.sensorid
                    WHERE
                        s.id = %s""", (scene,)
            )

        row = curs.fetchone()
        if row['sensor'].startswith('ETM+'):
            row['sensor'] = 'ETM+'

        path = os.path.join(self.job['data_path'], row['satellite'], row['sensor'], row['filename'])

        curs.close()
        conn.close()

        return path

    def computeSplits(self):
        # get scenes for job
        scenes = get_scenes(self.job)

        _ = self._iterator.next()  # TODO: don't send path for custom splits
        self._collector = Collector.Collector(self._connection)
        # get path for each scene and send split
        for s in scenes:
            path = get_path(self.job, s)
            print("py: sending split '{}'".format(path))
            sys.stdout.flush()
            self._collector.collect(path)
        self._collector._close()

    def deliver(self, path, collector):
        # get metadata for lvl0a
        metadata = get_metadata(self.job, path)
        key = str(metadata['id'])
        collector.collect((key, bytearray(pickle.dumps(metadata)), bytearray(1)))

        print("py: received path:", path)
        print("py: retrieved metadata:", metadata)
        sys.stdout.flush()


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
        print("Image:", value[0], self.count)
        self.count += 1
        return True


def main():
    try:
        data_path = sys.argv[1]
    except IndexError:
        data_path = "/data1/gfz-fe/GeoMultiSens/database/sampledata/"

    connection = {
            "database": "usgscache",
            "user": "gmsdb",
            "password": "gmsdb",
            "host": "localhost",
            "connect_timeout": 3,
            "options": "-c statement_timeout=10000"
        }

    inputFormat = GMSDB(data_path, connection, 26184107)

    env = get_environment()
    data = env.read_custom(data_path, ".*?\\.bsq", True, inputFormat, IMAGE_TUPLE)

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
