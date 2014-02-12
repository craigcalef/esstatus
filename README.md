esstatus
========

ElasticSearch Status Tool

In honor of the release of ElasticSearch 1.0 and in memory of all the operators
who had to deal with an ElasticSearch cluster and its less than stellar 
administration tool I now release the chingadera who got me through a many
sleepless nights and time wondering "wtf is my cluster doing?"  Hopefully 
the new `cat` plugin will make all of our wildest dreams come true.

Usage
=====

	Usage: esstatus.py [options]

	Options:
	  -h, --help         show this help message and exit
	  -s                 Show Indices Status
	  -u                 Show Unassigned Shards
	  -n                 Show Indices to Nodes
	  -r                 Show Indices Routing
	  -t                 Show Indices Routing as CSV
	  -c                 Show Cluster Health
	  -p INDICE_PATTERN  Regular expression for index pattern
	  --sleep=SLEEP      Seconds to sleep between updates

