#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=./linsort2GB.log
#SBATCH --wait-all-nodes=1

./linSort2GB.sh $SLURM_JOB_NODELIST
