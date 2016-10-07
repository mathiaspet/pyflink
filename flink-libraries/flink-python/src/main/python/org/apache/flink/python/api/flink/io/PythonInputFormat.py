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
import sys


class FileInputSplit(object):
    def __init__(self, path, start, end, hosts):
        self.path = path
        self.start = start
        self.end = end
        self.hosts = hosts


class PythonInputFormat(Function.Function):
    def __init__(self):
        super(PythonInputFormat, self).__init__()
        self.close_called = False

    def _run(self):
        self._iterator._setLargeTuples(False)
        collector = self._collector
        function = self.deliver
        print("before iterator next")
        sys.stdout.flush()
        split = self._iterator.next()
        print("after iterator next")
        print(split)
        print(type(split))
        print(type("close"))
        sys.stdout.flush()

        if split[0] == "close":
            self.close_called = True
        while split is not None and split[0] != "close":
            try:
                function(split, collector)
            except:
                print("in function call:", sys.exc_info()[0])
            try:
                self._iterator._reset()
            except:
                print("in iterator reset call:", sys.exc_info()[0])
                sys.stdout.flush()
            try:
                self._connection.send_end_signal()
            except:
                print("in connection end call:", sys.exc_info()[0])
                sys.stdout.flush()
            try:
                split = self._iterator.next()
            except:
                print("in iterator next call:" + str(self.close_called), sys.exc_info()[0])
                sys.stdout.flush()
            if split[0] == "close":
                print("close split encountered")
                sys.stdout.flush()
                self.close_called = True


        collector._close()

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
