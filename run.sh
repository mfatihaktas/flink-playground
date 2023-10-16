#!/bin/bash

if [ $1 = "e" ]; then
  python exp/exp_pyflink_mini_cluster.py

# elif [ $1 = "em" ]; then
#   python ...

else
  echo "Unexpected arg= ${1}"
fi
