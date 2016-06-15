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
from flink.plan.Constants import STRING, BYTES
from flink.spatial.Types import Tile



IMAGE_TUPLE = STRING, BYTES, BYTES


class TileToTuple(FlatMapFunction):
    def flat_map(self, value, collector):
        tile_meta = {
            "acquisitionDate": value._aquisitionDate,
            #"coordinates": (value._leftUpperLat, value._rightLowerLat, value._leftUpperLon, value._rightLowerLon),
            "width": value._width,
            "height": value._height,
            "band": value._band,
            "pathRow": value._pathRow,
            "xPixelWidth": value._xPixelWidth,
            "yPixelWidth": value._yPixelWidth
        }

        collector.collect((
            value._aquisitionDate,
            #self._meta_to_bytes(tile_meta),
            ImageWrapper._meta_to_bytes(tile_meta),
            value._content
        ))


class TupleToTile(FlatMapFunction):
    def flat_map(self, value, collector):
        image = ImageWrapper(value)

        as_tile = Tile()
        as_tile._aquisitionDate = image.acquisitionDate
        as_tile._pathRow = image.get_meta('pathRow')
        as_tile._leftUpperLat = image.coordinates[0]
        as_tile._leftUpperLon = image.coordinates[2]
        as_tile._rightLowerLat = image.coordinates[1]
        as_tile._rightLowerLon = image.coordinates[3]
        as_tile._width = image.get_meta('width')
        as_tile._height = image.get_meta('height')
        as_tile._band = image.get_meta('band')
        as_tile._xPixelWidth = image.get_meta('xPixelWidth')
        as_tile._yPixelWidth = image.get_meta('yPixelWidth')
        as_tile._content = image.content

        collector.collect(as_tile)


class ImageWrapper(object):
    def __init__(self, tup):
        self._tup = tup
        self._meta = self._meta_from_bytes(tup[1])

    @staticmethod
    def _meta_from_bytes(meta_bytes):
        all_strings = iter(meta_bytes.split(b"\0"))
        meta = dict()
        while True:
            try:
                key = next(all_strings)
            except StopIteration:
                break
            value = next(all_strings)
            meta[key.decode()] = value.decode()
        path = meta['wrs_path']
        row = meta['wrs_row']
        if len(row) == 2:
            row = '0'+row
        meta['pathRow'] = path+row
        return meta

    @staticmethod
    def _meta_to_bytes(meta):
        all_strings = []
        for k, v in meta.items():
            if(type(v) == type([])):
                #TODO: add proper hierarchichal value handling here; this is just a test to make something else work
                value = ""
                for s in v:
                    value += s
                all_strings.extend([k, value])
            else:
                all_strings.extend([k, v])
        all_strings.append("\0")
        return bytearray("\0".join(all_strings).encode())

    @staticmethod
    def from_data(coordinates, width, height, band, pathRow,
                  acquisitionDate, xPixelWidth, yPixelWidth):
        tile_meta = {
            "acquisitionDate": acquisitionDate,
            "coordinates": coordinates,
            "width": width,
            "height": height,
            "band": band,
            "pathRow": pathRow,
            "xPixelWidth": xPixelWidth,
            "yPixelWidth": yPixelWidth
        }

        return ImageWrapper((
            acquisitionDate,
            ImageWrapper._meta_to_bytes(tile_meta),
            bytearray(width * height * 2)
        ))

    def get_meta(self, name):
        return self._meta[name]

    def set_meta(self, name, value):
        self._meta[name] = value
        self._tup = (self._tup[0], self._meta_to_bytes(self._meta), self._tup[2])

    @property
    def acquisitionDate(self):
        return self._meta["acquisitiondate"]

    @acquisitionDate.setter
    def acquisitionDate(self, value):
        self.set_meta("acquisitiondate", value)

    @property
    def coordinates(self):
        return self._meta["coordinates"]

    @coordinates.setter
    def coordinates(self, value):
        self.set_meta("coordinates", value)

    @property
    def content(self):
        return self._tup[2]

    @property
    def s16_tile(self):
        # This only works on little endian systems
        return memoryview(self._tup[2]).cast("h")

    def get_coordinate(self, index):
        index = index // 2
        x = index % self.get_meta("width")
        y = index // self.get_meta("width")
        newLon = self.coordinates[2] + self.get_meta("xPixelWidth") * x
        newLat = self.coordinates[0] - self.get_meta("yPixelWidth") * y
        return (newLat, newLon)

    def get_index_from_coordinate(self, coord):
        lat, lon = coord
        latDiff = int(self.coordinates[0] - lat)
        lonDiff = int(lon - self.coordinates[2])

        if latDiff < 0 or lonDiff < 0:
            return -1

        x = lonDiff // self.get_meta("xPixelWidth")
        y = latDiff // self.get_meta("yPixelWidth")

        return int(y * self.get_meta("width") + x)
