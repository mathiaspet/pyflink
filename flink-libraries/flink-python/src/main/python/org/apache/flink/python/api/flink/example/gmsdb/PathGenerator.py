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
import os.path
import glob
import logging

from flink.example.gmsdb.misc import get_GMS_sensorcode


_obj_name_dic = {
    'AVNIR-2': None,
    'TM4': None,
    # 'TM5': '9fe4af95-a16e-45e6-9397-7085c74a30d8.dill', # 16.9. class. bayesian
    # 'TM5': 'xval_0.96_classicalBayesian_c723159c.dill', # 28.9. class. bayesian
    'TM5': 'xval_0.96_classicalBayesian_b84d087e.dill',  # 28.9. fastest classifier
    # 'TM7': '1c8560d9-8436-43e7-b170-416c15e732a7.dill', # ganz einfach
    # 'TM7': '38bc1428-2775-4d0c-a551-dcea99ff9046.dill',
    # 'TM7': '9fe4af95-a16e-45e6-9397-7085c74a30d8.dill', # 16.9. class. bayesian
    # 'TM7': 'xval_0.96_classicalBayesian_c723159c.dill', # 28.9. class. bayesian
    'TM7': 'xval_0.96_classicalBayesian_b84d087e.dill',  # 28.9. fastest classifier
    # 'LDCM': 'xval_0.97_classicalBayesian_3ac27853.dill', # 28.9. class. bayesian
    'LDCM': 'xval_0.97_classicalBayesian_305e7da1.dill',  # 28.9. fastest class. bayesian
    # 'LDCM': glob.glob(os.path.join(path_cloud_classifier_objects,'*.dill'))[0]
    #               if glob.glob(os.path.join(path_cloud_classifier_objects,'*.dill')) != [] else None,
    'SPOT1a': None,
    'SPOT1b': None,
    'SPOT2a': None,
    'SPOT2b': None,
    'SPOT3a': None,
    'SPOT3b': None,
    'SPOT4a': None,
    'SPOT4b': None,
    'SPOT5a': None,
    'SPOT5b': None,
    'RE5': None,
    'AST_V1': None,
    'AST_V2': None,
    'AST_S': None,
    'AST_T': None,
    'S2A10': None,
    'S2A20': None,
    'S2A60': None,
    'S2B10': None,
    'S2B20': None,
    'S2B60': None
}


class PathGenerator():
    """Methods return absolute paths corresponding to the input object.
    To be instanced with a GmsObject."""
    def __init__(self, gms):
        self.acquisition_date = gms.acquisition_date
        self.entity_ID = gms.entity_ID
        self.filename = gms.filename
        self.image_type = gms.image_type
        self.job = gms.job
        self.proc_level = gms.proc_level.name
        self.satellite = gms.satellite
        self.sensor = gms.sensor
        self.subsystem = gms.subsystem

        self.logger = logging.getLogger(__name__ + ".PathGenerator")

    def get_path_procdata(self):
        """Returns the target folder of all processed data for the current scene."""
        return os.path.join(self.job['path_procdata'], self.satellite, self.sensor,
                            self.acquisition_date.strftime('%Y-%m-%d'), self.entity_ID)

    def get_baseN(self):
        """Returns the basename belonging to the given scene."""
        if not self.subsystem:
            components = [self.sensor, self.entity_ID]
        else:
            components = [self.sensor, self.subsystem, self.entity_ID]
        return '__'.join(components)

    # def get_path_logfile(self):
    #     """Returns the path of the logfile belonging to the given scene, e.g. '/path/to/file/file.log'."""
    #     return os.path.join(self.get_path_procdata(), self.get_baseN()+'.log')

    def get_local_archive_path_baseN(self):  # must be callable from L0A-P
        """Returns the path of the downloaded raw data archive, e.g. '/path/to/file/file.tar.gz'."""
        filename = self.filename
        if self.image_type == 'RSD' and self.satellite:
            folder_rawdata = os.path.join(self.job['path_archive'], self.satellite, self.sensor)
            if os.path.exists(os.path.join(folder_rawdata, filename)):
                outP = os.path.join(folder_rawdata, filename)
            else:
                extensions_found = [ext for ext in ['.tar.gz', '.zip', '.hdf']
                                    if os.path.exists(os.path.join(folder_rawdata, filename + ext))]
                if extensions_found:
                    assert len(extensions_found) > 0, 'The dataset %s.* cannot be found at %s' % (filename, folder_rawdata)
                    assert len(extensions_found) == 1, "The folder %s contains multiple files identified as raw data to be " \
                           "processed. Choosing first one.." % folder_rawdata
                    outP = os.path.join(folder_rawdata, filename + extensions_found[0])
                else:
                    if filename.endswith('.SAFE') and \
                            os.path.exists(os.path.join(folder_rawdata, os.path.splitext(filename)[0]) + '.zip'):
                        outP = os.path.join(folder_rawdata, os.path.splitext(filename)[0]) + '.zip'  # FIXME Bug in Datenbank
                    else:
                        raise FileNotFoundError('The dataset %s.* cannot be found at %s' % (filename, folder_rawdata)) # TODO DOWNLOAD COMMAND
            return outP

        if self.image_type == 'DGM':
            if self.satellite and 'SRTM' in self.satellite.upper():
                return os.path.join(self.job['path_archive'], 'srtm2/', self.entity_ID + '_sub.bsq')
        if self.image_type == 'ATM':
            return os.path.join(self.job['path_archive'], 'atm_data/',  self.entity_ID + '.bsq')

        self.logger.critical('Given dataset specification is not yet supported. Specified parameters: image_type: %s; satellite: %s; sensor: %s',
                             self.image_type, self.satellite, self.sensor)

    def get_path_gmsfile(self):
        """Returns the path of the .gms file belonging to the given processing level, e.g. '/path/to/file/file.gms'."""
        return os.path.join(self.get_path_procdata(), '%s_%s.gms' % (self.get_baseN(), self.proc_level))

    def get_path_imagedata(self):
        """Returns the path of the .bsq file belonging to the given processing level, e.g. '/path/to/file/file.bsq'."""
        return os.path.join(self.get_path_procdata(), '%s_%s.bsq' % (self.get_baseN(), self.proc_level))

    def get_path_maskdata(self):
        """Returns the path of the *_masks_*.bsq file belonging to the given processing level,
        e.g. '/path/to/file/file_masks_L1A.bsq'."""
        return os.path.join(self.get_path_procdata(), '%s_masks_%s.bsq' % (self.get_baseN(), self.proc_level))

    def get_path_tempdir(self):
        path_archive = self.get_local_archive_path_baseN()
        RootName = os.path.splitext(os.path.basename(path_archive))[0]
        RootName = os.path.splitext(RootName)[0] if os.path.splitext(RootName)[1] else RootName
        assert sys.platform.startswith('linux')
        RootDir = "/dev/shm/GeoMultiSens"
        return os.path.join(RootDir, RootName, self.sensor, self.subsystem) \
            if self.subsystem not in [None, ''] else os.path.join(RootDir, RootName, self.sensor)

    def get_path_cloud_class_obj(self, get_all=False):
        """Returns the absolute path of the the training data used by cloud classifier."""
        sensorcode = get_GMS_sensorcode(self.satellite, self.sensor, self.subsystem)
        path_cloud_classifier_objects = os.path.join(self.job['path_cloud_classif'],
                                                     self.satellite, self.sensor)

        if get_all:  # returns a list
            list_clf = glob.glob(os.path.join(path_cloud_classifier_objects, '*.dill'))
            classifier_path = [os.path.join(path_cloud_classifier_objects, str(i))
                               for i in list_clf]
        else:
            classifier_path = os.path.join(path_cloud_classifier_objects,
                                           str(_obj_name_dic[sensorcode]))
        try:
            if isinstance(classifier_path, str):
                path4pathcheck = classifier_path
            elif isinstance(classifier_path, list):
                path4pathcheck = classifier_path[0]
            else:
                path4pathcheck = None

            if os.path.isfile(os.path.join(path_cloud_classifier_objects, path4pathcheck)):
                return classifier_path
            else:
                self.logger.warning('Cloud masking is not yet implemented for "%s" "%s".'
                                    % (self.satellite, self.sensor))
        except KeyError:
            self.logger.warning('Sensorcode "%s" is not included in sensorcode dictionary'
                                'and can not be converted into GMS sensorcode.' % sensorcode)
