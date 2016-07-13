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
import fnmatch
import os.path
import tarfile
import zipfile

import psycopg2
import psycopg2.extras
import xattr
import simplejson as json



def get_scenes(job):
    conn = psycopg2.connect(**job['connection'])
    curs = conn.cursor()

    curs.execute("""SELECT unnest(sceneids) FROM scenes_jobs WHERE id = %s""", (job['id'], ))
    scenes = [s[0] for s in curs]

    curs.close()
    conn.close()

    return scenes


def get_path(job, scene):
    conn = psycopg2.connect(**job['connection'])
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
                    s.id = %s""", (scene, )
        )

    row = curs.fetchone()
    if row['sensor'].startswith('ETM+'):
        row['sensor'] = 'ETM+'

    path = os.path.join(job['data_path'], row['satellite'], row['sensor'], row['filename'])

    curs.close()
    conn.close()

    return path

def get_hosts(path):
    #default return value:
    hosts = ("localhost",)

    attrs = xattr.list(path)
    if len(attrs) > 0:
        jsonString = xattr.get(path, "xtreemfs.locations")
        #jsonString = xattr.get("/home/mathiasp/mount/localScenes/227064_020717_BLA_SR.bsq", "xtreemfs.locations")
        parsed = json.loads(jsonString)
        print(parsed["replicas"][0]['osds'][0]['address'].split(":")[0])

        replicas = parsed["replicas"]
        hosts = ()
        for replica in replicas:
            #assume that scenes are not striped across osds
            osd = replica["osds"][0]
            address = osd['address'].split(":")[0]
            hosts += (address, )
    return hosts


def open_in_archive(path_archive, matching_expression, read_mode='r'):
    count_matching_files = 0
    if zipfile.is_zipfile(path_archive):
        archive = zipfile.ZipFile(path_archive, 'r')
        matching_files = fnmatch.filter(archive.namelist(), matching_expression)
        count_matching_files = len(matching_files)
        content_file = archive.read(matching_files[0])
        filename_file = os.path.join(path_archive, matching_files[0])
        archive.close()

    elif tarfile.is_tarfile(path_archive):
        archive = tarfile.open(path_archive, 'r|gz')
        for F in archive:
            if fnmatch.fnmatch(F.name, matching_expression):
                content_file = archive.extractfile(F)
                content_file = content_file.read()
                filename_file = os.path.join(path_archive, F.name)
                count_matching_files += 1
        archive.close()

    assert count_matching_files > 0, \
        'Matching expression matches no file. Please revise your expression!'
    assert count_matching_files == 1, \
        'Matching expression matches more than 1 file. Please revise your expression!'

    if isinstance(content_file, bytes) and read_mode == 'r':
        content_file = content_file.decode('latin-1')

    return content_file, filename_file


_obj_name_dic = {
        'AVNIR-2': None,
        'TM4': None,
        # 'TM5': '9fe4af95-a16e-45e6-9397-7085c74a30d8.dill', # 16.9. class. bayesian
        # 'TM5': 'xval_0.96_classicalBayesian_c723159c.dill', # 28.9. class. bayesian
        'TM5': 'xval_0.96_classicalBayesian_b84d087e.dill',  # 28.9. fastest classifier
        # 'TM7': '1c8560d9-8436-43e7-b170-416c15e732a7.dill', # ganz einfach
        # 'TM7': '38bc1428-2775-4d0c-a551-dcea99ff9046.dill',
        # 'TM7': '9fe4af95-a16e-45e6-9397-7085c74a30d8.dill', # 16.9. class. bayesian
        # 'TM7': 'xval_0.96_classicalBayesian_c723159c.dill', # 28.9. class. bayesian
        'TM7': 'xval_0.96_classicalBayesian_b84d087e.dill',  # 28.9. fastest classifier
        # 'LDCM': 'xval_0.97_classicalBayesian_3ac27853.dill', # 28.9. class. bayesian
        'LDCM': 'xval_0.97_classicalBayesian_305e7da1.dill',  # 28.9. fastest class. bayesian
        # 'LDCM': glob.glob(os.path.join(path_cloud_classifier_objects, '*.dill'))[0]
        #               if glob.glob(os.path.join(path_cloud_classifier_objects, '*.dill')) != [] else None,
        'S1a': None,
        'S1b': None,
        'S2a': None,
        'S2b': None,
        'S3a': None,
        'S3b': None,
        'S4a': None,
        'S4b': None,
        'S5a': None,
        'S5b': None,
        'RE5': None,
        'AST_V1': None,
        'AST_V2': None,
        'AST_S': None,
        'AST_T': None
    }


_sensorcodes = {
        'ALOS_AVNIR-2': 'AVNIR-2',
        'Landsat-4_TM': 'TM4',  # call from layerstacker
        'Landsat-4_TM_SAM': 'TM4',  # call from metadata object
        'Landsat-5_TM': 'TM5',
        'Landsat-5_TM_SAM': 'TM5',
        'Landsat-7_ETM+': 'TM7',
        'Landsat-7_ETM+_SAM': 'TM7',
        'Landsat-8_OLI_TIRS': 'LDCM',
        'Landsat-8_LDCM': 'LDCM',
        'SPOT-1_HRV1': 'S1a',  # MS
        'SPOT-1_HRV2': 'S1b',
        'SPOT-2_HRV1': 'S2a',
        'SPOT-2_HRV2': 'S2b',
        'SPOT-3_HRV1': 'S3a',
        'SPOT-3_HRV2': 'S3b',
        'SPOT-4_HRVIR1': 'S4a',
        'SPOT-4_HRVIR2': 'S4b',
        'SPOT-5_HRG1': 'S5a',  # PAN HRG2A
        'SPOT-5_HRG2': 'S5b',  # MS HRG2J
        'RapidEye-5_MSI': 'RE5',
        'SRTM_SRTM2': 'SRTM2',
        'Terra_ASTER_VNIR1': 'AST_V1',
        'Terra_ASTER_VNIR2': 'AST_V2',
        'Terra_ASTER_SWIR': 'AST_S',
        'Terra_ASTER_TIR': 'AST_T'
    }


def get_GMS_sensorcode(satellite, sensor, subsystem, logger=None):
    if satellite.startswith("SPOT") and sensor[-1] not in ['1', '2']:
        sensor = sensor[:-1]
    meta_sensorcode = "_".join(s for s in [satellite, sensor, subsystem] if s)
    try:
        return _sensorcodes[meta_sensorcode]
    except KeyError:
        if logger:
            logger.warning('Sensor %s is not included in sensorcode dictionary'
                           'and can not be converted into GMS sensorcode.'
                           % meta_sensorcode)
