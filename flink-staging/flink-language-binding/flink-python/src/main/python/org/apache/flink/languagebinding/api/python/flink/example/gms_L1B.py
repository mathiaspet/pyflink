#!/usr/bin/env python
# encoding: utf-8
import sys

from flink.plan.Environment import get_environment
from flink.plan.Constants import TILE
from flink.functions.FlatMapFunction import FlatMapFunction
from flink.functions.FilterFunction import FilterFunction


class Level_1B_Processor(FlatMapFunction):
    def flat_map(self, value, collector):
        collector.collect(value)


class ImageTypeFilter(FilterFunction):
    def filter(self, value):
        return value.image_type == 'RDS'


def main():
    env = get_environment()
    if len(sys.argv) != 8:
        print("Usage: ./bin/pyflink.sh EnviCube -<dop> <input directory> <left-upper-longitude> <left-upper-latitude> <block size> <pixel size> <output path>")
        sys.exit()

    dop = int(sys.argv[1])
    path = sys.argv[2]
    leftLong = sys.argv[3]
    leftLat = sys.argv[4]
    blockSize = sys.argv[5]
    pixelSize = sys.argv[6]
    outputPath = sys.argv[7]

    data = env.read_envi(path, leftLong, leftLat, blockSize, pixelSize)
    processed = data\
        .filter(ImageTypeFilter())\
        .flat_map(Level_1B_Processor(), TILE)

    processed.write_envi(outputPath)

    env.set_degree_of_parallelism(dop)

    env.execute(local=True)


if __name__ == "__main__":
    main()
