#!/usr/bin/env python
# -*- coding: utf-8 -*-
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
