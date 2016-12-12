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
from flink.functions import RuntimeContext, Function
import logging

logger = logging.getLogger(__name__)

class PythonOutputFormat(Function.Function):

    def __init__(self, path):
        super(PythonOutputFormat, self).__init__()
        self._path = path

    def _run(self):
        collector = self._collector
        function = self.write
        iterator = self._iterator
        try:
            for value in iterator:
                result = function(value)
                if result is not None:
                    for res in result:
                        collector.collect(res)
        except Exception:
            logger.exception("Error while executing output format flat map function")
            raise
        self.close()
        collector._close()

    def collect(self, value):
        collector = self._collector
        self.write(value)


    def write(self, value):
        pass

    def close(self):
        pass
