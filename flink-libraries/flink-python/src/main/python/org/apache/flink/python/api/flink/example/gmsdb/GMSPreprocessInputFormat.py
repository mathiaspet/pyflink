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
import os
import pickle
import sys

import psycopg2
import gdal
from flink.io.PythonInputFormat import PythonInputFormat, FileInputSplit
from flink.example.gmsdb.lvl0a import process as lvl0a
from flink.example.gmsdb.lvl0b import process as lvl0b
from flink.example.gmsdb.misc import get_path, get_scenes


class GMSDB(PythonInputFormat):
    def __init__(self, data_path, connection, job_id):
        super(GMSDB, self).__init__()
        gdal.AllRegister()  # TODO: register the ENVI driver only
        self.job = {
            'id': job_id,
            'data_path': data_path,
            'path_procdata': data_path,
            'path_archive': data_path,
            'connection': connection,
            'skip_pan': False,
            'skip_thermal': False
        }

        # get base pathnames
        conn = psycopg2.connect(**self.job['connection'])
        curs = conn.cursor()

        # TODO: these paths don't exist
        if not self.job['data_path']:
            curs.execute("SELECT value FROM config WHERE key = 'path_data_root'")
            self.job['data_path'] = curs.fetchone()[0]

        if not self.job['path_procdata']:
            curs.execute("SELECT value FROM config WHERE key = 'foldername_procdata'")
            self.job['path_procdata'] = os.path.join(self.job['data_path'], curs.fetchone()[0])

        if not self.job['path_archive']:
            curs.execute("SELECT value FROM config WHERE key = 'foldername_download'")
            self.job['path_archive'] = os.path.join(self.job['data_path'], curs.fetchone()[0])

        curs.close()
        conn.close()

        print("job:", self.job)
        print("finished init")
        sys.stdout.flush()

    def createInputSplits(self, minNumSplits, splitPath, collector):
        print("creating splits")
        sys.stdout.flush()
        # get scenes for job
        scenes = get_scenes(self.job)
        print("scenes are", scenes)
        sys.stdout.flush()

        # get path for each scene and send split
        for s in scenes:
            path = get_path(self.job, s)
            collector.collect(FileInputSplit(path, 0, 1, ("localhost",)))
            print("sent split for ", path)
            sys.stdout.flush()

    def deliver(self, split, collector):
        path = split[0]
        # get metadata for lvl0a
        lvl0a_data = lvl0a(self.job, path)
        if lvl0a_data is None:
            return
        lvl0b_data = lvl0b(self.job, lvl0a_data)
        key = str(lvl0b_data['id'])
        collector.collect((key, bytearray(pickle.dumps(lvl0b_data)), bytearray(1)))

        print("py: received path:", path)
        print("py: retrieved metadata:", lvl0b_data)
        sys.stdout.flush()
