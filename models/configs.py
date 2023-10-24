import os
from models.gcp_info import GCP_SPARK_MASTER_ADDRESS

HOME_PATH = os.path.expanduser('~')
PROJECT_NAME = 'SparkTuning'

SPARK_CONF_PATH = os.path.join(HOME_PATH, PROJECT_NAME, 'data/spark.conf')
SPARK_CONF_TEMPLATE_PATH = os.path.join(HOME_PATH, PROJECT_NAME, 'data/spark.conf.template')

MASTER_ADDRESS = GCP_SPARK_MASTER_ADDRESS
MASTER_CONF_PATH = os.path.join(HOME_PATH, 'HiBench/conf')
MASTER_BENCH_BASH = os.path.join(HOME_PATH, 'scripts/run_terasort.sh')

HIBENCH_REPORT_PATH = os.path.join(HOME_PATH, PROJECT_NAME, 'data/hibench.report')

INCUMBENTS_RESULTS_PATH = os.path.join(HOME_PATH, PROJECT_NAME, 'results')

baxus_params = {'embedding_type': 'baxus',
                'acquisition_function': 'ts',
                'mle_optimization': 'sample-and-choose-best',
                'target_dim': 10, # 10
                'n_init': 20, # 10
                'max_evals': 300, # 100
                'noise_std': 0,
                'new_bins_on_split': 3,
                'multistart_samples': 100,
                'mle_training_steps': 50,
                'multistart_after_samples': 10,
                'l_init': 0.8,
                'l_min': 0.5**7,
                'l_max': 1.6,
                'adjust_initial_target_dim': True,
                'budget_until_input_dim': 0
                }