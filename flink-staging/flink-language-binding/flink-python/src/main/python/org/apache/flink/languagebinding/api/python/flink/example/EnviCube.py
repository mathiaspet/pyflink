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
from struct import pack

from flink.plan.Environment import get_environment
from flink.plan.Constants import TILE, STRING
from flink.plan.Constants import Tile
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.GroupReduceFunction import GroupReduceFunction
from flink.functions.KeySelectorFunction import KeySelectorFunction


NOVAL = pack("<h", -9999)


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
    def __init__(self, leftUpper=(0, 0), rightLower=(0, 0), xSize=0, ySize=0):
        super(CubeCreator, self).__init__()
        self.leftUpperLat, self.leftUpperLon = leftUpper
        self.rightLowerLat, self.rightLowerLon = rightLower
        self.xSize = xSize
        self.ySize = ySize

    @property
    def leftUpper(self):
        return (self.leftUpperLat, self.leftUpperLon)

    @property
    def rightLower(self):
        return (self.rightLowerLat, self.rightLowerLon)

    def reduce(self, iterator, collector):
        """
        :param iterator:
        :param collector:
        :return:
        """
        # group tiles by band
        band_to_tiles = defaultdict(set)
        for tile in iterator:
            band_to_tiles[tile._band].add(tile)

        # iterate over bands in order
        bands = sorted(band_to_tiles.keys())
        orig_not_null_counter = 0
        inside_counter = 0
        known_counter = 0
        for b in bands:
            result = Tile()
            # Initialize content with -9999
            result._content = bytearray(self.xSize * self.ySize * 2)
            for i in range(0, len(result._content), 2):
                result._content[i] = NOVAL[0]
                result._content[i+1] = NOVAL[1]

            # iterate over tiles for current band
            updated = False
            for t in band_to_tiles[b]:
                if not updated:
                    result.update(self.leftUpper, self.rightLower, self.xSize,
                                  self.ySize, b, t._pathRow, t._aquisitionDate,
                                  t._xPixelWidth, t._yPixelWidth)
                    updated = True

                for i, (px_coord_lat, px_coord_lon) in coord_iter(t):
                    if t._content[i:i+2] != NOVAL:
                        orig_not_null_counter += 1

                    if (self.leftUpperLat >= px_coord_lat and
                            px_coord_lat >= self.rightLowerLat and
                            self.leftUpperLon <= px_coord_lon and
                            px_coord_lon <= self.rightLowerLon):
                        # get index in result tile for current pixel
                        index = int(result.get_content_index_from_coordinate((px_coord_lat, px_coord_lon)))
                        if index >= 0 and index < len(result._content):
                            inside_counter += 1
                            px_value = t._content[i:i+2]
                            if px_value != NOVAL:
                                known_counter += 1
                            result._content[index] = px_value[0]
                            result._content[index+1] = px_value[1]

                collector.collect(result)

        print("inside", inside_counter)
        print("known_counter", known_counter)
        print("orig not null", orig_not_null_counter)


class AcqDateSelector(KeySelectorFunction):
    def get_key(self, value):
        return value._aquisitionDate


def coord_iter(tile):
    lon = tile._leftUpperLon
    lat = tile._leftUpperLat
    yield (0, (lat, lon))
    if len(tile._content) > 2:
        for i in range(2, len(tile._content), 2):
            if i % tile._width == 0:
                lon = tile._leftUpperLon
                lat -= tile._yPixelWidth
            else:
                lon += tile._xPixelWidth
            yield (i, (lat, lon))


if __name__ == "__main__":
    print("found args length:", len(sys.argv))
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

    leftUpper = (float(leftLat), float(leftLong))
    rightLower = (float(leftLat) - int(blockSize) * int(pixelSize),
                  float(leftLong) + int(blockSize) * int(pixelSize))

    data = env.read_envi(path, leftLong, leftLat, blockSize, pixelSize)
    cube = data.group_by(AcqDateSelector(), STRING)\
        .reduce_group(CubeCreator(leftUpper, rightLower, int(blockSize), int(blockSize)), TILE)

    cube.write_envi(outputPath)

    env.set_degree_of_parallelism(dop)

    env.execute(local=True)
