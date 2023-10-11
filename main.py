import os
import pandas as pd
import numpy as np

import baxus
from baxus.util.behaviors import BaxusBehavior
from baxus.util.behaviors.gp_configuration import GPBehaviour
from baxus.baxus import BAxUS
from baxus.util.parsing import embedding_type_mapper, acquisition_function_mapper, mle_optimization_mapper

from envs.spark import SparkParameters
from models.bench import SparkBench

embedding_type = "baxus" # "hesbo"
acquisition_function = "ts" # "ei"
mle_optimization = "sample-and-choose-best" # "multistart-gd"

bin_sizing_method = embedding_type_mapper[embedding_type]
acquisition_function = acquisition_function_mapper[acquisition_function]
mle_optimization_method = mle_optimization_mapper[mle_optimization]

target_dim = 10 # args.target_dim
n_init = 10 # args.n_init # initial samples (pre-observed samples)
max_evals = 100 # args.max_evals
noise_std = 0 # args.noise_std
new_bins_on_split = None # args.new_bins_on_split
multistart_samples = 100 # args.multistart_samples
mle_training_steps = 50 # args.mle_training_steps
multistart_after_samples = 10 # args.multistart_after_sample
l_init = 0.8 # args.initial_baselength
l_min = 0.5**7 # args.min_baselength
l_max = 1.6 # args.max_baselength
adjust_initial_target_dim = True # args.adjust_initial_target_dimension
budget_until_input_dim = 0 # args.budget_until_input_dim

behavior = BaxusBehavior(n_new_bins=2,#new_bins_on_split,
                         initial_base_length=l_init,
                         min_base_length=l_min,
                         max_base_length=l_max,
                         acquisition_function=acquisition_function,
                         embedding_type=bin_sizing_method,
                         adjust_initial_target_dim=adjust_initial_target_dim,
                         noise=noise_std,
                         budget_until_input_dim=budget_until_input_dim
                        )

gp_behaviour = GPBehaviour(
    mll_estimation=mle_optimization_method,
    n_initial_samples=multistart_samples,
    n_best_on_lhs_selection=multistart_after_samples,
    n_mle_training_steps=mle_training_steps,
)

sp = SparkParameters()

input_dim = len(sp) # args.input_dim
ub = sp.ub
lb = sp.lb

f = SparkBench(dim=input_dim, ub=ub, lb=lb, sp=sp)

optim = BAxUS(f=f,
              n_init=n_init,
              max_evals=max_evals,
              target_dim=target_dim,
              behavior=behavior,
              gp_behaviour=gp_behaviour
              )

optim.optimize()
