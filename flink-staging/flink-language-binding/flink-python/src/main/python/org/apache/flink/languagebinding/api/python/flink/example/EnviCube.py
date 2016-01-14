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
from flink.plan.Constants import TILE
from flink.functions.GroupReduceFunction import GroupReduceFunction

from flink.example.ImageWrapper import TileToTuple, TupleToTile, ImageWrapper, IMAGE_TUPLE


NOVAL = pack("<h", -9999)


class CubeCreator(GroupReduceFunction):
    def __init__(self, leftUpper=(0, 0), rightLower=(0, 0), xSize=0, ySize=0):
        super(CubeCreator, self).__init__()
        self.leftUpperLat, self.leftUpperLon = leftUpper
        self.rightLowerLat, self.rightLowerLon = rightLower
        self.xSize = xSize
        self.ySize = ySize

    def reduce(self, iterator, collector):
        """
        :param iterator:
        :param collector:
        :return:
        """
        # group images by band
        band_to_tiles = defaultdict(set)
        for image in iterator:
            image = ImageWrapper(image)
            band_to_tiles[image.get_meta("band")].add(image)

        # iterate over bands in order
        bands = sorted(band_to_tiles.keys())
        orig_not_null_counter = 0
        inside_counter = 0
        known_counter = 0
        for b in bands:
            sample = band_to_tiles[b].pop()
            result = ImageWrapper.fromData(sample.coordinates, self.xSize, self.ySize, b,
                                           sample.get_meta("pathRow"), sample.acquisitionDate,
                                           sample.get_meta("xPixelWidth"), sample.get_meta("yPixelWidth"))
            band_to_tiles[b].add(sample)
            # Initialize content with -9999
            for i in range(0, len(result.content), 2):
                result.content[i] = NOVAL[0]
                result.content[i+1] = NOVAL[1]

            # iterate over tiles for current band
            for t in band_to_tiles[b]:
                for i, (px_coord_lat, px_coord_lon) in coord_iter(t):
                    if t.content[i:i+2] != NOVAL:
                        orig_not_null_counter += 1

                    if (self.leftUpperLat >= px_coord_lat and
                            px_coord_lat >= self.rightLowerLat and
                            self.leftUpperLon <= px_coord_lon and
                            px_coord_lon <= self.rightLowerLon):
                        # get index in result image for current pixel
                        index = int(result.get_content_index_from_coordinate((px_coord_lat, px_coord_lon)))
                        if index >= 0 and index < len(result._content):
                            inside_counter += 1
                            px_value = t._content[i:i+2]
                            if px_value != NOVAL:
                                known_counter += 1
                            result._content[index] = px_value[0]
                            result._content[index+1] = px_value[1]

            collector.collect(result._tup)

        print("inside", inside_counter)
        print("known_counter", known_counter)
        print("orig not null", orig_not_null_counter)


def coord_iter(image):
    lon = image.coordinates[0]
    lat = image.coordinates[2]

    width = image.get_meta("width")
    xPixelWidth = image.get_meta("xPixelWidth")
    yPixelWidth = image.get_meta("yPixelWidth")

    yield (0, (lat, lon))
    if len(image.content) > 2:
        for i in range(2, len(image.content), 2):
            if i % width == 0:
                lon = image.coordinates[0]
                lat -= yPixelWidth
            else:
                lon += xPixelWidth
            yield (i, (lat, lon))


if __name__ == "__main__":
    if len(sys.argv) != 8:
        print("Usage: ./bin/pyflink.sh EnviCube - <dop> <input directory> <left-upper-longitude> <left-upper-latitude> <block size> <pixel size> <output path>")
        sys.exit()
    env = get_environment()

    # prepare arguments
    dop = int(sys.argv[1])
    path = sys.argv[2]
    leftLon = float(sys.argv[3])
    leftLat = float(sys.argv[4])
    blockSize = int(sys.argv[5])
    pixelSize = int(sys.argv[6])
    outputPath = sys.argv[7]

    leftUpper = leftLat, leftLon
    rightLower = leftLat - blockSize * pixelSize, leftLon + blockSize * pixelSize

    data = env.read_envi(path, str(leftLon), str(leftLat), str(blockSize), str(pixelSize))

    tuples = data.flat_map(TileToTuple(), IMAGE_TUPLE)
    cube = tuples.group_by(0)\
                 .reduce_group(CubeCreator(leftUpper, rightLower, blockSize, blockSize), IMAGE_TUPLE)
    tiles = cube.flat_map(TupleToTile(), TILE)
    tiles.write_envi(outputPath)

    env.set_degree_of_parallelism(dop)
    env.execute(local=True)
