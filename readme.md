# Partitioning Scientific Workflows for Federated Execution

This repository conatains the source code for the master thesis " Partitioning Scientific Workflows for Federated Execution" by [Felix Kummer](https://github.com/Felix-Kummer).

This repositories consists of:

- [sources/](./sources): the original source code of [WorkflowSim](https://github.com/WorkflowSim/WorkflowSim-1.0)
	- several files in [workflowsim/](./sources/org/workflowsim/) have been modified to enable the simulation of federated systems 
- [master_code/](./master_code/): New code developed for the master thesis
	- [experiments/](./master_code/experiments/): Contains scripts and some results for the evaluation
	- [src/](./master_code/src/): Contains the additional source code for the thesis 
	- [lib/](./master_code/lib/): Contains jar's required for the new code in [src/](./master_code/src/)
- other files are the same as in the original [WorkflowSim](https://github.com/WorkflowSim/WorkflowSim-1.0) repository

# Usage
For installation and execution instructions for WorkflowSim, please refer to the original [WorkflowSim](https://github.com/WorkflowSim/WorkflowSim-1.0) repository.

To run the code from the master thesis, the files in [sources/](./sources/) and in [master_code/src](./master_code/src) need to be build with jars in [lib/](./lib) and [master_code/lib](./master_code/lib). For usage examples, you may refer to [evaluation.py](./master_code/experiments/evaluation.py) or [gridSearch.py](master_code/experiments/gridSearch.py).