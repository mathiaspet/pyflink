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
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction


class L11Processor(FlatMapFunction):
    # represents the map steps until the reduce
    def flat_map(self, value, collector):
        # value is a tile here that has been splitted as a last step in the preceeding IF
        # TODO: make adjustments here
        value = self.toaReflectance(value)
        value = self.addNoDataMask(value)
        value = self.addCloudMask(value)

        collector.collect(value)

    def toaReflectance(self, tile):
        # TODO: implement me
        return tile

    def addNoDataMask(self, tile):
        # TODO: implement me
        return tile

    def addCloudMask(self, tile):
        # TODO: implement me
        return tile


class CornerpointAdder(GroupReduceFunction):
    # represents the reduce step that adds the corner points of a scene
    # TODO: change into combine
    def reduce(self, iterator, collector):
        for i in iterator:
            # TODO: implement me
            # Question: does this function return a scene or a set of tiles?
            collector.collect(i)


class L12Processor(FlatMapFunction):
    # represents the map steps after the reduce
    def flat_map(self, value, collector):
        value = self.addAqcuisitionDate(value)
        value = self.addVAAAqcuisitionAngle(value)
        value = self.addOrbitParams(value)
        value = self.applyNoDataMask(value)
        collector.collect(value)

    def setTrueCornerPoints(self, value):
        # TODO: implement me
        return value

    def addAqcuisitionDate(self, value):
        # TODO: implement me
        return value

    def addVAAAqcuisitionAngle(self, value):
        # TODO: implement me
        return value

    def addOrbitParams(self, value):
        # TODO: implement me
        return value

    def applyNoDataMask(self, value):
        # TODO: implement me
        return value
