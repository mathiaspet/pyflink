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
from collections import deque
from flink.connection import Connection, Iterator, Collector
from flink.functions import RuntimeContext

class PythonOutputFormat(object):

    def __init__(self):
        self._connection = None
        self._iterator = None
        self.context = None
        self._nextRun = True
        self._userInit()

    def _userInit(self):
        pass

    def _run(self):
        function = self.deliver
        for value in self._iterator:
            if value is not None:
                if value != "close":
                    function(value)
                    self._connection.send_end_signal()
                else:
                    print "received close message"
                    self._nextRun = False
                    self._collector._close()
        self._connection.reset()


    def deliver(self, input):
        pass


    def _configure(self, input_file, output_file, port):
        self._connection = Connection.BufferingTCPMappedFileConnection(input_file, output_file, port)
        self._iterator = Iterator.Iterator(self._connection)
        self.context = RuntimeContext.RuntimeContext(self._iterator, None)

    def close(self):
        self.close()

    def _go(self):
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
