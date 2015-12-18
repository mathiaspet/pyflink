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
import marshal


def tile_to_tuple(tile):
    tile_meta = {
        "acquisitionDate": tile._aquisitionDate,
        "coordinates": (tile._leftUpperLat, tile._rightLowerLat, tile._leftUpperLon, tile._rightLowerLon),
        "width": tile._width,
        "height": tile._height,
        "band": tile._band,
        "pathRow": tile._pathRow,
        "xPixelWidth": tile._xPixelWidth,
        "yPixelWidth": tile._yPixelWidth
    }

    return (
        tile._aquisitionDate,
        bytearray(marshal.dumps(tile_meta)),
        tile._content
    )


class ImageWrapper(object):
    def __init__(self, tup):
        self._tup = tup
        self._meta = marshal.loads(tup[1])

    @staticmethod
    def fromData(coordinates, width, height, band, pathRow,
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
            bytearray(marshal.dumps(tile_meta)),
            bytearray(width * height * 2)
        ))

    def get_meta(self, name):
        return self._meta[name]

    def set_meta(self, name, value):
        self._meta[name] = value
        self._tup = (self._tup[0], bytearray(marshal.dumps(self._meta)), self._tup[2])

    @property
    def acquisitionDate(self):
        return self._meta["acquisitionDate"]

    @acquisitionDate.setter
    def acquisitionDate(self, value):
        self.set_meta("acquisitionDate", value)

    @property
    def coordinates(self):
        return self._meta["coordinates"]

    @coordinates.setter
    def coordinates(self, value):
        self.set_meta("coordinates", value)

    @property
    def content(self):
        return self._tup[2]

    def get_coordinate(self, index):
        index = index // 2
        x = index % self.get_meta("width")
        y = index // self.get_meta("width")
        newLon = self.coordinates[2] + self.get_meta("xPixelWidth") * x
        newLat = self.coordinates[0] - self.get_meta("yPixelWidth") * y
        return (newLat, newLon)

    def get_content_index_from_coordinate(self, coord):
        lat, lon = coord
        latDiff = int(self.coordinates[2] - lat)
        lonDiff = int(lon - self.coordinates[0])

        if latDiff < 0 or lonDiff < 0:
            return -1

        x = lonDiff // self.get_meta("xPixelWidth")
        y = latDiff // self.get_meta("yPixelWidth")

        return int(2 * (y * self.get_meta("width") + x))
