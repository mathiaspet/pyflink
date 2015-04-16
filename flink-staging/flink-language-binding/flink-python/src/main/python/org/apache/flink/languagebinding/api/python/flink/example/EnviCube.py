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
from collections import defaultdict

from flink.plan.Environment import get_environment
from flink.plan.Constants import TILE
from flink.plan.Constants import Tile
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.KeySelectorFunction import KeySelectorFunction


NOVAL = b'\xf1\xd8'


#deprecated
class Tokenizer(FlatMapFunction):
    def flat_map(self, value, collector):
        print("acquisition date: " + value.aquisitionDate + " " + value.pathRow)
        print("band " + str(value.band) + " lu (" + str(value.leftUpperLon) + ", " + str(value.leftUpperLat) + ")")
        print(" rl (" + str(value.rightLowerLon) + ", " + str(value.rightLowerLat) + ")")
        print("pixelsize: " + str(value.xPixelWidth) + " x " + str(value.yPixelWidth))
        print("tile size: " + str(value.width) + " x " + str(value.height))
        collector.collect(value)


class CubeCreator(GroupReduceFunction):
    def __init__(self):
        self.xSize = 0
        self.ySize = 0
        self.leftUpper = (0, 0)
        self.rightLower = (0, 0)

    def reduce(self, iterator, collector):
        """
        :param iterator:
        :param collector:
        :return:
        """
        # group tiles by band
        band_to_tiles = defaultdict(set)
        for tile in iterator:
            band_to_tiles[tile.band].add(tile)

        # iterate over bands in order
        bands = sorted(band_to_tiles.keys())
        orig_not_null_counter = 0
        inside_counter = 0
        known_counter = 0
        for b in bands:
            result = Tile()
            # Initialize content with -9999 = 0xf1d8
            # TODO: Does this allocate all values and then copy them?
            result.content = bytearray(self.x_size * self.y_size * NOVAL)

            # iterate over tiles for current band
            updated = False
            for t in band_to_tiles[b]:
                if not updated:
                    result.update(self.left_upper, self.right_lower, self.x_size,
                                  self.y_size, b, t.path_row, t.acq_date,
                                  t.x_px_width, t.y_px_width)
                    updated = True

                # iterate over tile content 2 bytes per iteration
                for i in range(0, len(t.content), 2):
                    if t.content[i:i+2] != NOVAL:
                        orig_not_null_counter += 1

                    # check coordinates of current pixel
                    px_coord = t.get_coordinate(i)
                    if (self.left_upper.lat >= px_coord.lat and
                            px_coord.lat >= self.right_lower.lat and
                            self.left_upper.lon <= px_coord.lon and
                            px_coord.lon <= self.right_lower.lon):
                        # get index in result tile for current pixel
                        index = result.get_content_index_from_coordinate(px_coord)
                        if index >= 0 and index < len(result.content):
                            inside_counter += 1
                            px_value = t.content[i:i+2]
                            if px_value != NOVAL:
                                known_counter += 1
                            result.content[index] = px_value[0]
                            result.content[index+1] = px_value[1]

                collector.collect(result)

        print("inside", inside_counter)
        print("known_counter", known_counter)
        print("orig not null", orig_not_null_counter)


class AcqDateSelector(KeySelectorFunction):
    def get_key(self, value):
        return value.aquisitionDate

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
    cube = data.group_by(AcqDateSelector(), (TILE)).reduce_group(CubeCreator(),(TILE))

    cube.write_envi(outputPath)

    env.set_degree_of_parallelism(1)

    env.execute(local=True)
