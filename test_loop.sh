#!/bin/bash

while [ 1 ]
do
python graphite_migrate.py -s $1
sleep 10
done

