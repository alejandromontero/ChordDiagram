#!/usr/bin/python

import sys
import os
import numpy 
import collections
import json 

## Script that modifies the original extracted Log from AOP4Hadoop.
## HDFS events are extracted and modified to build charts.
## If no path is specified, the script will look for the log at the following path: 
## /home/alejandro/Documents/AOP4Hadoop/AOP2/AOP4Hadoop/dist/hadoop-1.0.3/logs/DFSLOG.log

def main(path =  "/home/alejandro/Documents/AOP4Hadoop/AOP2/AOP4Hadoop/dist/hadoop-1.0.3/logs/DFSLOG.log"):
	print "LogFile Path:", path
	positions = {}
	counter = 0
	total = 0
	with open (path, "r", 1) as f:
		for line in f:
			li = line.split(",")
			if li[4] != 'W':
				if li[5] not in positions: 
					positions[li[5]] = counter
					counter += 1
				elif li[6] not in positions:
					positions[li[6]] = counter
					counter += 1
				total += long(li[8])

	matrix = numpy.zeros((len(positions),len(positions)))

 	with open (path, "r", 1) as f:
		for line in f:
			li = line.split(",")
			if li[4] != 'W': 
				matrix[positions[li[5]]][positions[li[6]]] += long(li[8])

	for k in matrix: 
		for v in range (len(k)):
			k[v] = round((k[v]/total) * 100, 10)

	fil = open("/home/alejandro/Documents/charts/chord/Chord-Diagram-Storytelling-gh-pages/data/log.txt","w",1)
	for k in matrix:
		coun = 0
		for v in range (len(k)):
			if coun == len(positions) - 1: fil.write(str(k[v]))
			else:
				fil.write(str(k[v])+",")
				coun += 1
		fil.write('\n')
	fil.close()

	fil = open("/home/alejandro/Documents/charts/chord/Chord-Diagram-Storytelling-gh-pages/data/datanodes.txt","w",1)
	keys = collections.OrderedDict(sorted(positions.items()))
	for k,v in keys.iteritems():
		fil.write('"' + k + '",')

def usage():
	print("Usage: ./changeLog.py pathToLog")

if len(sys.argv) > 2: usage()
elif len(sys.argv) == 2: main(sys.argv[1])
else: main()