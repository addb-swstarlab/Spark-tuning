import os
from models.configs import *
from baxus.benchmarks.benchmark_function import Benchmark

class SparkBench(Benchmark):
    def __init__(self, dim, ub, lb, sp, bench_type=None):
        super().__init__(dim=dim, ub=ub, lb=lb, noise_std=0)
        self.sp = sp # spark parameter infos
        self.bench_type = bench_type
    
    def __call__(self, x):
        self.sp.save_configuration_file(x)
        self.execute_spark_bench()
        self.res, _ = self.get_results()
        
        return res
        
    def execute_spark_bench(self):
        # transport a generated configuration to the master server
        print(f'sshpass scp {SPARK_CONF_PATH} {MASTER_ADDRESS}:{MASTER_CONF_PATH}')
        os.system(f'sshpass scp {SPARK_CONF_PATH} {MASTER_ADDRESS}:{MASTER_CONF_PATH}')
        os.system(f'sshpass ssh {MASTER_ADDRESS} {MASTER_BENCH_BASH}')
    
    def get_results(self):
        f = open(HIBENCH_REPORT_PATH, 'r')
        report = f.readlines()
        f.close()
        duration = report[-1].split()[-3]
        tps = report[-1].split()[-2]
        return duration, tps
