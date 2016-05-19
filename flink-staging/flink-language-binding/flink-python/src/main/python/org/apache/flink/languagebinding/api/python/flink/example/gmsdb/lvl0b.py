#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os.path


def process(job, metadata):
    assert os.path.isfile(metadata['path']) and not os.path.isdir(metadata['path'])

    if metadata['image_type'] == 'RSD' and 'oli' in metadata['sensor']:
        metadata['georef'] = True
    else:
        metadata['georef'] = False

    return metadata
