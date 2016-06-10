#!/usr/bin/env python
# -*- coding: utf-8 -*-
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


def get_scenes(job):
    conn = psycopg2.connect(**job['connection'])
    curs = conn.cursor()

    curs.execute("""SELECT unnest(sceneids) FROM scenes_jobs WHERE id = %s""", (job['id'],))
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
                    s.id = %s""", (scene,)
        )

    row = curs.fetchone()
    if row['sensor'].startswith('ETM+'):
        row['sensor'] = 'ETM+'

    path = os.path.join(job['data_path'], row['satellite'], row['sensor'], row['filename'])

    curs.close()
    conn.close()

    return path


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

    assert count_matching_files > 0,\
        'Matching expression matches no file. Please revise your expression!'
    assert count_matching_files == 1,\
        'Matching expression matches more than 1 file. Please revise your expression!'

    if isinstance(content_file, bytes) and read_mode == 'r':
        content_file = content_file.decode('latin-1')

    return content_file, filename_file


class PathGenerator:
    def __init__(self, job, metadata):
        self.proc_level = metadata['proc_level'] or ''
        self.image_type = metadata['image_type'] or ''
        self.satellite = metadata['satellite'] or ''
        self.sensor = metadata['sensor'] or ''
        self.subsystem = metadata['subsystem'] or ''
        self.AcqDate = metadata['acquisitiondate'] or ''
        self.entity_ID = metadata['entityid'] or ''
        self.job = job

    def get_path_procdata(self):
        return os.path.join(self.job['path_procdata'], self.satellite, self.sensor,
                            self.AcqDate.strftime('%Y-%m-%d'), self.entity_ID)

    def get_baseN(self):
        if not self.subsystem:
            return '__'.join([self.sensor, self.entity_ID])
        else:
            return '__'.join([self.sensor, self.subsystem, self.entity_ID])

    def get_path_logfile(self):
        return os.path.join(self.get_path_procdata(), self.get_baseN()+'.log')

    def get_local_archive_path_baseN(self):  # must be callable from L0A-P
        if self.image_type == 'RSD' and self.satellite is not None:
            folder_rawdata = os.path.join(self.job['path_archive'], self.satellite, self.sensor)
            for ext in ['.tar.gz', '.zip', '.hdf']:
                archive = os.path.join(folder_rawdata, self.entity_ID + ext)
                if os.path.exists(archive):
                    return archive

            raise AssertionError("Can't find dataset for %s at %s"
                                 % (self.entity_ID, folder_rawdata))

        if self.image_type == 'DGM' and self.satellite and 'SRTM' in self.satellite.upper():
            return os.path.join(self.job['path_archive'], 'srtm2/', self.entity_ID+'_sub.bsq')

        if self.image_type == 'ATM':
            return os.path.join(self.job['path_archive'], 'atm_data/', self.entity_ID + '.bsq')

        # # unsupported image type
        raise AssertionError('Given dataset specification is not yet supported. '
                             'Specified parameters: image_type: %s; satellite: %s; sensor: %s'
                             % (self.image_type, self.satellite, self.sensor))

    def get_path_gmsfile(self):
        return os.path.join(self.get_path_procdata(),
                            '%s_%s.gms' % (self.get_baseN(), self.proc_level))

    def get_path_imagedata(self):
        return os.path.join(self.get_path_procdata(),
                            '%s_%s.bsq' % (self.get_baseN(), self.proc_level))
