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
from abc import ABCMeta, abstractmethod
import sys
from collections import deque
from flink.connection import Connection, Iterator, Collector
from flink.plan.OperationInfo import OperationInfo
from flink.functions import RuntimeContext
from flink.plan.Constants import BYTES, STRING


class PythonInputFormat(object):
    
    def __init__(self):
        self._connection = None
        self._iterator = None
        self._collector = None
        self.context = None
        self._chain_operator = None
        self._env = None
        self._info = None
        self._nextRun = True
        self._userInit()

    def _userInit(self):
        pass

    def _run(self):
        collector = self._collector
        function = self.deliver
        for value in self._iterator:
            if value is not None:
                if value != "close_streamer":
                    function(value, collector)
                    self._connection.send_end_signal()
                else:
                    self._nextRun = False
                    self.close()

        self._connection.reset()

    def _chain(self, operator):
        self._chain_operator = operator

    def deliver(self, path, collector):
        pass

    def _configure(self, input_file, output_file, port, env, info):
        self._connection = Connection.BufferingTCPMappedFileConnection(input_file, output_file, port)
        self._iterator = Iterator.Iterator(self._connection, env)
        self._collector = Collector.Collector(self._connection, env, info)
        self.context = RuntimeContext.RuntimeContext(self._iterator, self._collector)
        self._env = env
        self._info = info
        if info.chained_info is not None:
            info.chained_info.operator._configure_chain(self.context, self._collector, info.chained_info)
            self._collector = info.chained_info.operator

    def _configure_chain(self, context, collector, info):
        self.context = context
        if info.chained_info is None:
            self._collector = collector
        else:
            self._collector = info.chained_info.operator
            info.chained_info.operator._configure_chain(context, collector, info.chained_info)


    def close(self):
        self._collector._close()

    def computeSplits(self):
        pass

    def _go(self):
        command = self._iterator.next()
        self._iterator._reset()
        self._connection.reset()
        if command is not None and command == "compute_splits":
            info = OperationInfo()
            info.types = [STRING, STRING]
            self._collector = Collector.Collector(self._connection, self._env, info)
            self.computeSplits()
            self._connection.send_end_signal()
        else:
            self._receive_broadcast_variables()
            while(self._nextRun):
                self._run()



    def _receive_broadcast_variables(self):
        broadcast_count = self._iterator.next()
        self._iterator._reset()
        self._connection.reset()
        for _ in range(broadcast_count):
            name = self._iterator.next()
            self._iterator._reset()
            self._connection.reset()
            bc = deque()
            while(self._iterator.has_next()):
                bc.append(self._iterator.next())
            self.context._add_broadcast_variable(name, bc)
            self._iterator._reset()
            self._connection.reset()
