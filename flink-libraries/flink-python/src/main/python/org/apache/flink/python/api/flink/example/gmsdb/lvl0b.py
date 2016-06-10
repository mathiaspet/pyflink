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
import os.path

from flink.example.gmsdb.misc import PathGenerator


def process(job, lvl0a_data):
    assert os.path.isfile(lvl0a_data['path']) and not os.path.isdir(lvl0a_data['path'])

    print('lvl0b for', lvl0a_data)

    path_gen = PathGenerator(job, lvl0a_data)
    lvl0a_data['baseN'] = path_gen.get_baseN()
    lvl0a_data['path_procdata'] = path_gen.get_path_procdata()
    lvl0a_data['path_logfile'] = path_gen.get_path_logfile()
    lvl0a_data['path_archive'] = path_gen.get_local_archive_path_baseN()
    # self.logger = HLP_F.setup_logger('log__' + self.baseN, self.path_logfile, self.job_CPUs, append=0)
    # path_gen = PG.path_generator(self.__dict__)  # passes a logger in addition to previous attributes

    if lvl0a_data['image_type'] == 'RSD' and 'oli' in lvl0a_data['sensor']:
        lvl0a_data['georef'] = True
    else:
        lvl0a_data['georef'] = False

    return lvl0a_data
