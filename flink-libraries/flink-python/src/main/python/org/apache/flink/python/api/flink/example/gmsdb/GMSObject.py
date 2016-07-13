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
from collections import OrderedDict
from copy import copy
from enum import IntEnum
import logging
import os.path
import re

import psycopg2
import psycopg2.extras

from flink.example.gmsdb.misc import open_in_archive
from flink.example.gmsdb.PathGenerator import PathGenerator


_sensor_re = re.compile("<SENSOR_CODE>([a-zA-Z0-9]*)</SENSOR_CODE>", re.I)


class ProcLevel(IntEnum):
    L0A = 1
    L0B = 2
    L1A = 3
    L1B = 4
    L1C = 5


class GmsObject(object):
    @classmethod
    def get_level0a_objects(job, path):
        dataset = GmsObject(job, path)
        dataset.level0a()

        if "sentinel-2a" in dataset.satellite.lower():
            for sys in ['S2A10', 'S2A20', 'S2A60']:
                sub = copy(dataset)
                sub.subsystem = sys
                yield sub
        elif "terra" in dataset.satellite.lower():
            for sys in ['VNIR1', 'VNIR2', 'SWIR', 'TIR']:
                sub = copy(dataset)
                sub.subsystem = sys
                yield sub
        else:
            yield dataset

    def __init__(self, job, path):
        self.job = job
        self.path = path
        self.filename = os.path.basename(path)
        self.proc_level = None
        self.logger = logging.getLogger(__name__)

    def level0a(self):
        conn = psycopg2.connect(**self.job['connection'])
        curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        # Get basic data from db
        curs.execute(
            """SELECT
                    s.id,
                    s.datasetid,
                    s.entityid,
                    s.acquisitiondate,
                    s.filename,
                    p.proc_level,
                    d.image_type,
                    sat.name AS satellite,
                    sen.name AS sensor,
                    sub.name AS subsystem
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
                    s.filename = %s""", (self.filename,))

        dataset = curs.fetchone()

        if not dataset:  # db returned no result
            return

        # NULL in db -> None
        self.acquisition_date = dataset["acquisitiondate"]
        self.entity_ID = dataset["entityid"]
        self.image_type = dataset["image_type"]
        # self.proc_level = dataset["proc_level"]  # ignore, process from 0
        self.satellite = dataset["satellite"]
        self.scene_ID = dataset["sceneid"]
        self.sensor = dataset["sensor"]
        self.subsystem = dataset["subsystem"]

        # return without setting proc level -> no further processing will be done
        if self.job.skip_thermal and self.subsystem == "TIR":
            return

        self.sensormode = GmsObject.lvl0a_get_sensormode()
        if "SPOT" in self.satellite and self.sensormode not in ['P', 'M']:
            return

        if "ETM+" in self.sensor:
            self.sensor = "ETM+"

        self.proc_level = ProcLevel.L0A

    def lvl0a_get_sensormode(self):
        if 'SPOT' in self.satellite:
            # FIXME: Don't read file
            path_archive = self.path
            dim, _ = open_in_archive(path_archive, '*/scene01/metadata.dim')
            SPOT_mode = _sensor_re.search(dim).group(1)

            if SPOT_mode in ['J', 'X', 'XS']:
                return 'M'
            else:
                return 'P'
        else:
            return 'M'

    def level0b(self):
        if self.proc_level != ProcLevel.L0B - 1:
            return

        # # Flink checks file existence
        # logger = logging.getLogger(__name__ + ".level0b")
        # if not os.path.isfile(self.path) or os.path.isdir(self.path):
        #     logger.warning("No data archive for %s dataset '%s' at %s.",
        #                    self.sensor, self.entity_ID, self.path)
        #     raise FileNotFoundError()

        pathgen = PathGenerator(self)
        self.baseN = pathgen.get_baseN()
        self.path_procdata = pathgen.get_path_procdata()
        self.extractedFolder = pathgen.get_path_tempdir()
        self.path_archive = pathgen.get_local_archive_path_baseN()
        self.path_cloud_class_obj = pathgen.get_path_cloud_class_obj(self.job["bench_CLD_class"])
        self.path_archive_valid = True  # set according to archive
        # self.path_logfile  # no logfiles

        self.georef = self.image_type == 'RSD' and 'oli' in self.sensor

        self.logger.info('Level 0B object for %s% s %s (data-ID %s) successfully created.',
                         self.satellite, self.sensor, self.subsystem, self.entity_ID)

        self.proc_level = ProcLevel.L0B

    @property
    def GMS_identifier(self):
        return OrderedDict([
            ('image_type', self.image_type),
            ('Satellite', self.satellite),
            ('Sensor', self.sensor),
            ('Subsystem', self.subsystem)
            ('logger', None)
        ])
