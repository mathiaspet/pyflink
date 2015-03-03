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
#  limitations under the License.
################################################################################
from flink.functions import Function
from flink.utilities import concat


class KeySelectorFunction(Function.Function):
    def __init__(self, pair=True):
        super(KeySelectorFunction, self).__init__()
        self._pair = pair

    def _configure(self, input_file, output_file, port):
        super(KeySelectorFunction, self)._configure(input_file, output_file, port)
        if not self._pair:
            self._run = self._run_append
            self.collect = self._collect_append

    def _run(self):
        collector = self._collector
        for value in self._iterator:
            collector.collect((self.get_key(value), value))
        collector._close()

    def _run_append(self):
        collector = self._collector
        for value in self._iterator:
            collector.collect(concat(self.get_key(value), value))
        collector._close()

    def collect(self, value):
        self._collector.collect((self.get_key(value), value))

    def _collect_append(self, value):
        self._collector.collect(concat(self.get_key(value), value))


    def get_key(self, value):
        pass
