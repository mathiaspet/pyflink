#!/usr/bin/env python
# encoding: utf-8
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
from collections import namedtuple

class Tile(namedtuple('Tile', [
    'acquisitionDate', 'pathRow', 'leftUpperLat', 'leftUpperLon',
    'rightLowerLat', 'rightLowerLon', 'width', 'height', 'band',
    'xPixelWidth', 'yPixelWidth', 'content'
    ])):

    __slots__ = ()

    @staticmethod
    def new():
        default = Tile('', '', 0.0, 0.0, 0.0, 0.0, -1, -1, -1, 0, 0, bytearray())
        return default

    def update(self, leftUpper, rightLower, width, height, band,
               pathRow, acquisitionDate, xPixelWidth, yPixelWidth):
        leftUpperLat, leftUpperLon = leftUpper
        rightLowerLat, rightLowerLon = rightLower

        self.acquisitionDate = acquisitionDate
        self.pathRow = pathRow
        self.leftUpperLat = leftUpperLat
        self.leftUpperLon = leftUpperLon
        self.rightLowerLat = rightLowerLat
        self.rightLowerLon = rightLowerLon
        self.width = width
        self.height = height
        self.band = band
        self.xPixelWidth = xPixelWidth
        self.yPixelWidth = yPixelWidth

    def get_coordinate(self, index):
        index /= 2
        x = index % self.width
        y = index // self.width
        newLon = self.leftUpperLon + self.xPixelWidth * x
        newLat = self.leftUpperLat + self.yPixelWidth * y
        return (newLat, newLon)

    def get_content_index_from_coordinate(self, coord):
        lat, lon = coord
        latDiff = int(self.leftUpperLat - lat)
        lonDiff = int(lon - self.leftUpperLon)

        if latDiff < 0 or lonDiff < 0:
            return -1

        x = lonDiff // self.xPixelWidth
        y = latDiff // self.yPixelWidth

        return 2 * (y * self.width + x)
