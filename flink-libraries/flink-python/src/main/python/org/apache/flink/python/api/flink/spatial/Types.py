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

class Tile(object):
    """
    Represents an envi tile (a blocked sub matrix of a scene in bsq)
    """
    def __init__(self):
        self._aquisitionDate = ""
        self._pathRow = ""
        self._leftUpperLat = 0.0
        self._leftUpperLon = 0.0
        self._rightLowerLat = 0.0
        self._rightLowerLon = 0.0
        self._width = -1
        self._height = -1
        self._band = -1
        self._xPixelWidth = 0.0
        self._yPixelWidth = 0.0
        self._content = bytearray()

    def update(self, leftUpper, rightLower, width, height, band,
               pathRow, acquisitionDate, xPixelWidth, yPixelWidth):
        leftUpperLat, leftUpperLon = leftUpper
        rightLowerLat, rightLowerLon = rightLower

        self._aquisitionDate = acquisitionDate
        self._pathRow = pathRow
        self._leftUpperLat = leftUpperLat
        self._leftUpperLon = leftUpperLon
        self._rightLowerLat = rightLowerLat
        self._rightLowerLon = rightLowerLon
        self._width = width
        self._height = height
        self._band = band
        self._xPixelWidth = xPixelWidth
        self._yPixelWidth = yPixelWidth

    def get_coordinate(self, index):
        index = index//2
        x = index % self._width
        y = index // self._width
        newLon = self._leftUpperLon + self._xPixelWidth * x
        newLat = self._leftUpperLat - self._yPixelWidth * y
        return (newLat, newLon)

    def get_content_index_from_coordinate(self, coord):
        lat, lon = coord
        latDiff = int(self._leftUpperLat - lat)
        lonDiff = int(lon - self._leftUpperLon)

        if latDiff < 0 or lonDiff < 0:
            return -1

        x = lonDiff // self._xPixelWidth
        y = latDiff // self._yPixelWidth

        return int(2 * (y * self._width + x))

class Cube(object):
    def __init__(self):
        """

        :return:
        """

class TimeSeries(object):
    def __init__(self):
        """

        :return:
        """
