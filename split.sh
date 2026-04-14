#!/bin/bash
echo 'splitting file combined-48s-r1-s56.csv'
split /dbdata/combined-48s-r1-s56.csv --number=l/100 --additional-suffix=_56.csv -d /dbdata/chunks/chunk_
echo 'splitting file combined-48s-r2-s60.csv'
split /dbdata/combined-48s-r2-s60.csv --number=l/200 --additional-suffix=_60.csv -d /dbdata/chunks/chunk_
echo 'splitting file combined-48s-r3-s64.csv'
split /dbdata/combined-48s-r3-s64.csv --number=l/2000 --additional-suffix=_64.csv -d /dbdata/chunks/chunk_