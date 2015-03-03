# ###############################################################################
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
from flink.plan.Constants import _Fields, WriteMode

class OperationInfo(dict):
    def __init__(self, env, info=None):
        super(OperationInfo, self).__init__()
        if info is None:
            self[_Fields.PARENT] = None
            self[_Fields.OTHER] = None
            self[_Fields.IDENTIFIER] = None
            self[_Fields.FIELD] = None
            self[_Fields.ORDER] = None
            self[_Fields.KEYS] = None
            self[_Fields.KEY1] = None
            self[_Fields.KEY2] = None
            self[_Fields.TYPES] = None
            self[_Fields.TYPE1] = None
            self[_Fields.TYPE2] = None
            self[_Fields.OPERATOR] = None
            self[_Fields.META] = None
            self[_Fields.NAME] = None
            self[_Fields.COMBINE] = False
            self[_Fields.DELIMITER_LINE] = "\n"
            self[_Fields.DELIMITER_FIELD] = ","
            self[_Fields.WRITE_MODE] = WriteMode.NO_OVERWRITE
            self[_Fields.PATH] = None
            self[_Fields.VALUES] = None
            self[_Fields.COMBINEOP] = None
            self[_Fields.CHILDREN] = []
            self[_Fields.SINKS] = []
            self[_Fields.PROJECTIONS] = []
            self[_Fields.BCVARS] = []
            self[_Fields.ID] = env._counter
            env._counter += 1
            self[_Fields.TO_ERR] = False
            self[_Fields.DISCARD1] = False
            self[_Fields.DISCARD2] = False
        else:
            self.update(info)