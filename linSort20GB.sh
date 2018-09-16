#!/bin/bash

	START_TIME=$SECONDS
	LC_ALL=C sort -o /tmp/mpateloutputDefault /input/data-20GB.in
	ELAPSED_TIME=$(($SECONDS - $START_TIME))
	./valsort /tmp/mpateloutputDefault
	echo "Time taken by LinSort: $ELAPSED_TIME"

