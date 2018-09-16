#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=./mysort2GB.log
#SBATCH --wait-all-nodes=1

./sortBench2GB.sh $SLURM_JOB_NODELIST
