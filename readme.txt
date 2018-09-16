To compile the files use following command-

make all

There are 2 .c files for 2GB and 20GB sorting program
However, the code writen in both are almost same just the define variable values are changed and a little change is in threading and merging part.

To run the file, use the commands -
./mySortBenchmark2
./mySortBenchmark20

To run the batch file, use the commands - 
sbatch sortBench2GB.slurm
sbatch sortBench20GB.slurm
sbatch linSort2GB.slurm
sbatch linSort20GB.slurm

The command line output will be saved in the following log files-
mysort2GB.log
mysort20GB.log
linsort2GB.log
linsort20GB.log

The .sh file contains execute command for program and then check the sorting using valsort file
