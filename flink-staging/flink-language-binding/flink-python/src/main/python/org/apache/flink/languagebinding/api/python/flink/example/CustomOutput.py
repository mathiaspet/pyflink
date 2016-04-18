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
import sys

from flink.plan.Environment import get_environment
from flink.plan.Constants import INT, STRING, BYTES
from flink.io.PythonOutputFormat import PythonOutputFormat
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.FlatMapFunction import FlatMapFunction
import os, sys, time, gdal
import numpy as np
from gdalconst import *
import __builtin__ as builtins



class CustomOF(PythonOutputFormat):
    def _userInit(self):
        print "userInit"

    def deliver(self, value):
        print "deliver", value

class Tokenizer(FlatMapFunction):
    def flat_map(self, value, collector):
        for word in value.lower().split():
            collector.collect((1, word))


class Adder(GroupReduceFunction):
    def reduce(self, iterator, collector):
        count, word = iterator.next()
        count += sum([x[0] for x in iterator])
        collector.collect((count, word))

if __name__ == "__main__":
    env = get_environment()

    data = env.from_elements("hello","world","hello","car","tree","data","hello")

    result = data \
        .flat_map(Tokenizer(), (INT, STRING)) \
        .group_by(1) \
        .reduce_group(Adder(), (INT, STRING), combinable=True)

    result.write_custom("/opt/path/", CustomOF())

    env.set_degree_of_parallelism(1)

    env.execute(local=True)