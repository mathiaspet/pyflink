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
from flink.connection import Connection, Iterator, Collector
from flink.functions import RuntimeContext, Function

class FileInputSplit(object):
    def __init__(self, path, start, end, hosts):
        self.path = path
        self.start = start
        self.end = end
        self.hosts = hosts


class PythonInputFormat(Function.Function):
    def __init__(self):
        super(PythonInputFormat, self).__init__()

    def _run(self):
        collector = self._collector
        function = self.deliver
        split = self._iterator.next()
        while split is not None:
            function(split, collector)
            self._iterator._reset()
            collector._close()
            split = self._iterator.next()

    def deliver(self, path, collector):
        pass

    def computeSplits(self, env, con):
        iterator = Iterator.PlanIterator(con, env)
        collector = Collector.SplitCollector(con, env)

        min_num_splits = iterator.next()
        path = iterator.next()

        self.createInputSplits(min_num_splits, path, collector)

        collector._close()
    
    def createInputSplits(self, minNumSplits, path, collector):
        pass
