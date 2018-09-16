#!/bin/bash
#SBATCH --nodes=1
#SBATCH --output=./linsort20GB.log
#SBATCH --wait-all-nodes=1

./linSort20GB.sh $SLURM_JOB_NODELIST
