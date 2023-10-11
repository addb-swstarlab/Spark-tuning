import os
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
        os.system('sshpass scp spark.conf jieun@34.64.230.62:/home/jieun/HiBench/conf')
        os.system('sshpass ssh jieun@34.64.230.62 /home/jieun/scripts/run_terasort.sh')
    
    def get_results(self):
        f = open('hibench.report', 'r')
        report = f.readlines()
        f.close()
        duration = report[-1].split()[-3]
        tps = report[-1].split()[-2]
        return duration, tps
