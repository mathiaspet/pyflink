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
from flink.plan.Constants import TILE, STRING
from flink.plan.Constants import Tile
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.KeySelectorFunction import KeySelectorFunction


#deprecated
class Tokenizer(FlatMapFunction):
    def flat_map(self, value, collector):
        print("acquisition date: " + value._aquisitionDate + " " + value._pathRow)
        print("band " + str(value._band) + " lu (" + str(value._leftUpperLon) + ", " + str(value._leftUpperLat) + ")")
        print(" rl (" + str(value._rightLowerLon) + ", " + str(value._rightLowerLat) + ")")
        print("pixelsize: " + str(value._xPixelWidth) + " x " + str(value._yPixelWidth))
        print("tile size: " + str(value._width) + " x " + str(value._height))
        collector.collect(value)


class CubeCreator(GroupReduceFunction):
    def reduce(self, iterator, collector):
        """
        :param iterator:
        :param collector:
        :return:
        """
        print("just received data in reduce")

class AcqDateSelector(KeySelectorFunction):
    def get_key(self, value):
        print("got key selector function")
        return value._aquisitionDate

if __name__ == "__main__":
    print("found args length: " + str(len(sys.argv)))
    env = get_environment()
    if len(sys.argv) != 8:
        print("Usage: ./bin/pyflink.sh EnviCube - <dop> <input directory> <left-upper-longitude> <left-upper-latitude> <block size> <pixel size> <output path>")
        sys.exit()
    
    dop = int(sys.argv[1])
    path = sys.argv[2]
    leftLong = sys.argv[3]
    leftLat = sys.argv[4]
    blockSize = sys.argv[5]
    pixelSize = sys.argv[6]
    outputPath = sys.argv[7]
    
    
    data = env.read_envi(path, leftLong, leftLat, blockSize, pixelSize)
    cube = data.group_by(AcqDateSelector(), STRING).reduce_group(CubeCreator(), TILE)
    
    cube.write_envi(outputPath)
    
    env.set_degree_of_parallelism(1)

    env.execute(local=True)
