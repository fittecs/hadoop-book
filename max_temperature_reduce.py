#!/usr/bin/env python

"""
$ cd ~/Documents/workspace/hadoop-book
$ hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.2.0.jar \
-input input/ncdc/all \
-output output \
-mapper ch02/src/main/python/max_temperature_map.py \
-reducer ch02/src/main/python/max_temperature_reduce.py \
-file ch02/src/main/python/max_temperature_map.py \
-file ch02/src/main/python/max_temperature_reduce.py
"""


import sys

(last_key, max_val) = (None, -sys.maxint)
for line in sys.stdin:
  (key, val) = line.strip().split("\t")
  if last_key and last_key != key:
    print "%s\t%s" % (last_key, max_val)
    (last_key, max_val) = (key, int(val))
  else:
    (last_key, max_val) = (key, max(max_val, int(val)))

if last_key:
  print "%s\t%s" % (last_key, max_val)