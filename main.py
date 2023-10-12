import os
import pandas as pd
import numpy as np
import argparse

import baxus
from baxus.util.behaviors import BaxusBehavior
from baxus.util.behaviors.gp_configuration import GPBehaviour
from baxus.baxus import BAxUS
from baxus.util.parsing import embedding_type_mapper, acquisition_function_mapper, mle_optimization_mapper

from envs.spark import SparkParameters
from models.utils import get_logger
from models.bench import SparkBench
from models.configs import baxus_params as bp

# parser = argparse.ArgumentParser()
# parser.add_argument('-t', '--test', type=str, default='test', help='description for this parameter')
# parser.add_argument('-i', '--ints', type=int, default=5, help='description for this parameter')
# parser.add_argument('-f', '--floats', type=float, default=0.5, help='description for this parameter')
logger = get_logger()

# opt = parser.parse_args()
# logger.info("## ARGUMENT INFORMATION ##")
# for _ in vars(opt):
#     logger.info(f"{_}: {vars(opt)[_]}")

def main():
    bin_sizing_method = embedding_type_mapper[bp['embedding_type']]
    acquisition_function = acquisition_function_mapper[bp['acquisition_function']]
    mle_optimization_method = mle_optimization_mapper[bp['mle_optimization']]

    behavior = BaxusBehavior(n_new_bins=2,#new_bins_on_split,
                            initial_base_length=bp['l_init'],
                            min_base_length=bp['l_min'],
                            max_base_length=bp['l_max'],
                            acquisition_function=acquisition_function,
                            embedding_type=bin_sizing_method,
                            adjust_initial_target_dim=bp['adjust_initial_target_dim'],
                            noise=bp['noise_std'],
                            budget_until_input_dim=bp['budget_until_input_dim']
                            )

    gp_behaviour = GPBehaviour(
        mll_estimation=mle_optimization_method,
        n_initial_samples=bp['multistart_samples'],
        n_best_on_lhs_selection=bp['multistart_after_samples'],
        n_mle_training_steps=bp['mle_training_steps'],
    )

    sp = SparkParameters()

    input_dim = len(sp) # args.input_dim
    ub = sp.ub
    lb = sp.lb

    f = SparkBench(dim=input_dim, ub=ub, lb=lb, sp=sp)

    optim = BAxUS(f=f,
                n_init=bp['n_init'],
                max_evals=bp['max_evals'],
                target_dim=bp['target_dim'],
                behavior=behavior,
                gp_behaviour=gp_behaviour
                )

    optim.optimize()

if __name__ == '__main__':
    try:
        main()
    except:
        logger.exception("ERROR!!")
    else:
        logger.handlers.clear()