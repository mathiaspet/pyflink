#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os.path
import re

import psycopg2

from flink.example.gmsdb.misc import open_in_archive


sensor_re = re.compile("<SENSOR_CODE>([a-zA-Z0-9]*)</SENSOR_CODE>", re.I)


def get_sensormode(dataset):
    if 'SPOT' in dataset['satellite']:
        path_archive = dataset['path']
        dim, _ = open_in_archive(path_archive, '*/scene01/metadata.dim')
        SPOT_mode = sensor_re.search(dim).group(1)

        assert SPOT_mode in ['J', 'X', 'XS', 'A', 'P', 'M'],\
            'Unknown SPOT sensor mode: %s' % SPOT_mode

        if SPOT_mode in ['J', 'X', 'XS']:
            return 'M'
        else:
            return 'P'
    else:
        return 'M'


def process(job, path):
    filename = os.path.basename(path)
    conn = psycopg2.connect(**job['connection'])
    curs = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

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
                    s.filename = %s""", (filename,))

    metadata = dict(curs.fetchone())
    metadata['path'] = path
    metadata['sensormode'] = get_sensormode(metadata)

    curs.close()
    conn.close()

    if 'ETM+' in metadata['sensor']:
        metadata['sensor'] = 'ETM+'
    # if metadata['subsystem'] is None:
    #     metadata['subsystem'] = ''

    # TODO: apply as filter because files were already promised as splits?
    # if job.skip_thermal and metadata['subsystem'] == 'TIR':
    #     continue   # removes ASTER TIR in case of skip_thermal
    # if job.skip_pan and metadata['sensormode'] == 'P':
    #     continue  # removes e.g. SPOT PAN in case of skip_pan

    return metadata
