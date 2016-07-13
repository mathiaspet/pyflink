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
import sys, glob, os
import xattr
import simplejson as json


def main():
    print("main")
    sys.stdout.flush()
    hosts = ("localhost",)
    attrs = xattr.list("/opt/gms_sample/227064_000202_BLA_SR.bsq")
    #print(attrs)
    if len(attrs) > 0:
        jsonString = xattr.get("/opt/gms_sample/227064_000202_BLA_SR.bsq", "xtreemfs.locations")
        #jsonString = xattr.get("/home/mathiasp/mount/localScenes/227064_020717_BLA_SR.bsq", "xtreemfs.locations")
        parsed = json.loads(jsonString)
        print(parsed["replicas"][0]['osds'][0]['address'].split(":")[0])
        hosts = ()
        replicas = parsed["replicas"]
        for replica in replicas:
            #assume that scenes are not striped across osds
            osd = replica["osds"][0]
            address = osd['address'].split(":")[0]
            hosts += (address, )

    print(hosts)

if __name__ == "__main__":
    main()

