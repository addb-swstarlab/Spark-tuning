import logging
from models.configs import SPARK_CONF_TEMPLATE_PATH

# dictionary = {'parameter_name': [[min, max], default]}
CORES = 8

class SparkParameters():
    def __init__(self):
        self.boolean_parameters = {'spark.broadcast.compress': [['true', 'false'], 'true'],
                                  'spark.memory.offHeap.enabled': [['true', 'false'], 'true'],
                                  'spark.rdd.compress': [['true', 'false'], 'true'],
                                  'spark.shuffle.compress': [['true', 'false'], 'true'],
                                  'spark.shuffle.spill.compress': [['true', 'false'], 'true'],
                                  'spark.sql.codegen.aggregate.map.twolevel.enable': [['true', 'false'], 'true'],
                                  'spark.sql.inMemoryColumnarStorage.compressed': [['true', 'false'], 'true'],
                                  'spark.sql.inMemoryColumnarStorage.partitionPruning': [['true', 'false'], 'true'],
                                  'spark.sql.join.preferSortMergeJoin': [['true', 'false'], 'true'],
                                  'spark.sql.retainGroupColumns': [['true', 'false'], 'true'],
                                  'spark.sql.sort.enableRadixSort': [['true', 'false'], 'true']
                                  }

        self.unit_mb_parameters = {'spark.broadcast.blockSize': [[1, 16], 4],
                                   'spark.driver.memory': [[1024, 4096], 1024],
                                   'spark.executor.memory': [[1024, 4096], 1024], # memory + memoryOverhead >= server memory
                                   'spark.executor.memoryOverhead': [[0, 8192], 384], # memory + memoryOverhead >= server memory
                                   'spark.kryoserializer.buffer.max': [[8, 128], 64],
                                   'spark.memory.offHeap.size': [[0, 49152], 0],
                                   'spark.reducer.maxSizeInFlight': [[24, 144], 48],
                                   'spark.shuffle.accurateBlockThreshold': [[100, 1000], 100],
                                   'spark.shuffle.service.index.cache.size': [[10, 2048], 100],
                                   'spark.storage.memoryMapThreshold': [[1, 10], 1]
                                   }

        self.unit_kb_parameters = {'spark.io.compression.zstd.bufferSize': [[16, 96], 32],
                                   'spark.kryoserializer.buffer': [[2, 128], 64],
                                   'spark.shuffle.file.buffer': [[16, 96], 32],
                                   'spark.sql.autoBroadcastJoinThreshold': [[1024, 8192], 1024]
                                   }

        self.numerical_parameters = {'spark.default.parallelism':[[CORES, 1000], CORES],
                                     'spark.driver.cores': [[1, CORES], 1],
                                     'spark.executor.cores': [[1, CORES], 3],
                                     'spark.executor.instances': [[2, 112], 2],
                                     'spark.io.compression.zstd.level': [[1, 5], 1],
                                     'spark.locality.wait': [[1, 6], 3],
                                     'spark.scheduler.revive.interval': [[1, 5], 1],
                                     'spark.shuffle.io.numConnectionsPerPeer': [[1, 5], 1],
                                     'spark.shuffle.sort.bypassMergeThreshold': [[100, 1000], 200],
                                     'spark.speculation.interval': [[10, 1000], 100],
                                     'spark.sql.cartesianProductExec.buffer.in.memory.threshold': [[1024, 8192], 4096],
                                     'spark.sql.codegen.maxFields': [[50, 200], 100],
                                     'spark.sql.inMemoryColumnarStorage.batchSize': [[5000, 20000], 10000],
                                     'spark.sql.shuffle.partitions': [[100, 1000], 200]
                                     }

        self.continous_parameters = {'spark.memory.fraction': [[0.5, 0.9], 0.6],
                                     'spark.memory.storageFraction': [[0.5, 0.9], 0.5]
                                     }
        
        self._get_combined_parameters()
        self._get_min_max_array()
        self._get_configuration_template()
        
        self.len_boolean = len(self.boolean_parameters)
        self.len_unit_kb = len(self.unit_kb_parameters)
        self.len_unit_mb = len(self.unit_mb_parameters)
        self.len_numerical = len(self.numerical_parameters)
        self.len_continuous = len(self.continous_parameters)
        
        self.parameter_names = list(self.all_parameters.keys())
        
    def __len__(self):
        return len(self.all_parameters)
    
    def _get_combined_parameters(self):
        self.all_parameters = self.boolean_parameters.copy()
        self.all_parameters.update(self.unit_kb_parameters)
        self.all_parameters.update(self.unit_mb_parameters)
        self.all_parameters.update(self.numerical_parameters)
        self.all_parameters.update(self.continous_parameters)
        
    def _get_min_max_array(self):
        self.ub = list()
        self.lb = list()
        for k, v in self.all_parameters.items():
            if k in self.boolean_parameters:
                self.lb.append(0)
                self.ub.append(1)
            else:
                self.lb.append(v[0][0])
                self.ub.append(v[0][1])
    
    def _get_configuration_template(self):
        f = open(SPARK_CONF_TEMPLATE_PATH, 'r')
        self.conf_template = f.readlines()
        f.close()
    
    def save_configuration_file(self, values):
        conf_file = open('spark.conf', 'w')
        conf_file.writelines(self.conf_template)
        logging.info('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        for i, p in enumerate(self.parameter_names):
            if p in self.boolean_parameters:
                v = 'true' if round(values[i])==1 else 'false'
            elif p in self.continous_parameters:
                v = round(values[i], 1)
            elif p in self.unit_kb_parameters:
                v = str(round(values[i]))+'k'
            elif p in self.unit_mb_parameters:
                v = str(round(values[i]))+'m'
            else:
                v = round(values[i])
            
            conf_file.writelines(f'{p}={v}\n')
            logging.info(f'{p}={v}')
        logging.info('$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        conf_file.close()
