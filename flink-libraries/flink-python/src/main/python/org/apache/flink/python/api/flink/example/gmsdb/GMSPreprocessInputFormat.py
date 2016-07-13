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
from flink.example.gmsdb.GMSObject import GmsObject
from flink.example.gmsdb.misc import get_path, get_scenes, get_hosts


class GMSDB(PythonInputFormat):
    def __init__(self, data_path, connection, job_id):
        super(GMSDB, self).__init__()
        gdal.AllRegister()  # TODO: register the ENVI driver only
        self.job = {
            'id': job_id,
            'data_path': data_path,  # TODO: see imports from db
            'path_procdata': data_path,
            'path_archive': data_path,
            'path_cloud_classif': data_path,
            'connection': connection,
            'skip_pan': False,
            'skip_thermal': False,
            'bench_CLD_class': False
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
            hosts = get_hosts(path)
            #collector.collect(FileInputSplit(path, 0, 1, ("localhost",)))
            collector.collect(FileInputSplit(path, 0, 1, hosts))
            print("sent split for ", path)
            sys.stdout.flush()


    def deliver(self, split, collector):
        path = split[0]
        # single file may lead to multiple objects (for subsystems)
        for image in GmsObject.get_level0a_objects(self.job, path):
            image.level0a()
            key = str(image.scene_ID)

            # TODO: write wrapper
            collector.collect((key, bytearray(pickle.dumps(image.__dict__)), bytearray(1)))

        print("py: received path:", path)
        print("py: retrieved metadata:", image)
        sys.stdout.flush()
