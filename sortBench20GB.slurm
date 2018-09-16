#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=./mysort20GB.log
#SBATCH --wait-all-nodes=1

./sortBench20GB.sh $SLURM_JOB_NODELIST
