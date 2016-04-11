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
from flink.functions.FilterFunction import FilterFunction

# from flink.example.ImageWrapper import TileToTuple, TupleToTile, ImageWrapper, IMAGE_TUPLE


class Filter(FilterFunction):
    def __init__(self):
        super(Filter, self).__init__()
        self.count = 0

    def filter(self, value):
        self.count += 1
        print("Image:", self.count)
        return True


def main():
    if len(sys.argv) != 4:
        print("Usage: ./bin/pyflink.sh ImageTest - <dop> <input directory> <output directory>")
        sys.exit()
    env = get_environment()

    dop = int(sys.argv[1])
    path = sys.argv[2]
    output_path = sys.argv[3]

    tuples = env.read_image_tuple(path).filter(Filter())
    tuples.write_image_tuple(output_path)

    env.set_degree_of_parallelism(dop)
    env.execute(local=True)


if __name__ == "__main__":
    main()
