#!/usr/bin/python2.6
# ElasticSearch Status Toy
# vim:ts=2 sw=2 expandtab
__author__ = "Craig Calef <craig.calef@thecontrolgroup.com>"
import requests, pprint, sys, re, curses, time, os
from pprint import pprint
from optparse import OptionParser

parser = OptionParser()
parser.add_option("-s", dest="show_indices_status", action="store_true", help="Show Indices Status")
parser.add_option("-u", dest="show_unassigned", action="store_true", help="Show Unassigned Shards")
parser.add_option("-n", dest="show_indices_nodes", action="store_true", help="Show Indices to Nodes")
parser.add_option("-r", dest="show_indices_routing", action="store_true", help="Show Indices Routing")
parser.add_option("-t", dest="show_indices_routing_csv", action="store_true", help="Show Indices Routing as CSV")
parser.add_option("-c", dest="cluster_health", action="store_true", help="Show Cluster Health")
parser.add_option("-p", dest="indice_pattern", help="Regular expression for index pattern", default=".*")
parser.add_option("--sleep", dest="sleep", help="Seconds to sleep between updates", default=2, type="int")
(options, args) = parser.parse_args()

soopretty = False
sleep_time = options.sleep 
cluster = 'localhost:9200'
indice_pattern = options.indice_pattern

BLUE = '\033[94m'
GREEN = '\033[92m'
YELLOW = '\033[93m'
RED = '\033[91m'
ENDC = '\033[0m'

def cluster_health():
  es_info = requests.get("http://%s/_cluster/health" % cluster).json()
  if es_info['status'] == 'yellow':
    es_info['status'] = YELLOW + "YELLOW" + ENDC
  if es_info['status'] == 'red':
    es_info['status'] = RED + "RED" + ENDC
  if es_info['status'] == 'green':
    es_info['status'] = GREEN + "GREEN" + ENDC
  ch = es_info.items()
  return es_info.items()

def indices_status():
  es_info = requests.get("http://%s/_cluster/health?level=shards" % cluster).json()
  for i,iv in es_info['indices'].items():
    print i, iv['status']
  # filter(lambda x: 'person' in x[0], es_info['indices'].items())

#persons = ",".join(["person_%c" % c for c in map(chr, range(ord('a'), ord('z')+1))])
#es_info = requests.get("http://%s/%s/_status" % (cluster, persons)).json()

def indices_by_pattern(pattern):
  es_info = requests.get("http://%s/_status" % cluster).json()
  return [indice for indice in es_info['indices'].keys() if re.match(pattern, indice)]

def unassigned_shards(pattern):
  es_info = requests.get("http://%s/_cluster/state" % cluster).json()
  for ua in es_info['routing_nodes']['unassigned']:
    print ua['index'], ua['shard'], ua['primary'], ua['state']

def indices_nodes(pattern):
  es_info = requests.get("http://%s/_status" % cluster).json()
  nodes = set()
  for indice in es_info['indices']:
    if re.match(pattern, indice):
      for shardid,shardinfo in es_info['indices'][indice]['shards'].items():
        for shard in shardinfo:
          nodes.add(shard['routing']['node'])
  #pprint(nodes)
  
  # First build a mapping of nodeid to concrete hostname
  cluster_nodes = requests.get("http://%s/_cluster/nodes" % cluster).json()
  for node in nodes:
    print cluster_nodes['nodes'][node]['hostname']

def indices_routing(pattern):
  # First build a mapping of nodeid to concrete hostname
  nodes = {}
  node_hostnames_list = []
  node_total_shards = {}
  cluster_nodes = requests.get("http://%s/_cluster/nodes" % cluster).json()
  for node in cluster_nodes['nodes'].keys():
    nodes[node] = cluster_nodes['nodes'][node]['hostname']
    node_hostnames_list.append(cluster_nodes['nodes'][node]['hostname'])
    #print node, cluster_nodes['nodes'][node]['hostname']
  # /indices/*/shards/0../*/routing/[node|state|primary]
  es_info = requests.get("http://%s/_status" % cluster).json()
  node_hostnames_list.sort()
  if options.show_indices_routing_csv:
    print ",", ",".join(node_hostnames_list)
  for indice in es_info['indices']:
    if re.match(pattern, indice):
      node_shards = {}
      for shardid,shardinfo in es_info['indices'][indice]['shards'].items():
        if options.show_indices_routing_csv:
          for shard in shardinfo:
            node_shards[nodes[shard['routing']['node']]] = shardid
        else:
          for shard in shardinfo:
            print indice, shardid, nodes[shard['routing']['node']], shard['routing']['node'], shard['routing']['state'], shard['routing']['primary']
        shard_node = nodes[shard['routing']['node']]
        node_total_shards[shard_node] = node_total_shards.get(shard_node, 0) + 1

    if options.show_indices_routing_csv:
      print indice, ",".join([node_shards.get(n, "") for n in node_hostnames_list])
  if options.show_indices_routing_csv:
    print "Total Shards,",",".join([str(node_total_shards.get(n, "")) for n in node_hostnames_list])
  #pprint(nodes)
 
def doc_counts(indices):
  es_info = requests.get("http://%s/%s/_status" % (cluster, indices)).json()
  idxdocs = []
  for i in es_info['indices'].keys():
    idxdocs.append((i, es_info['indices'][i]['docs']['num_docs'], es_info['indices'][i]['store']['size_in_bytes']))
  return idxdocs

# I just felt like messing around with curses...
if soopretty:
  myscreen = curses.initscr()
  curses.savetty()
  myscreen.border()
  myscreen.scrollok(True) 
  curses.noecho()

def pp(line):
  if soopretty:
    myscreen.addstr(line+"\n")
  else:
    print line

def index_documents():
	match_indices = ",".join(indices_by_pattern(indice_pattern))
	dc = doc_counts(match_indices)
	dc.sort(key=lambda x: x[0])
 
# Constantly refresh a simple two column data structure with an optionally calculated 'rate'
def updater_panel(columns, data_function):
  last_sample = {}
  last_ts = None

	try:
		while True:
			total = 0
			total_rate = 0
			pp("Getting cluster information... ")
			ts = time.time()
			if last_ts:
				pp("Last update took %d seconds" % (ts - last_ts))
			if soopretty:
				myscreen.clear()
				myscreen.border()
				myscreen.hline(3,1,chr(205),80)
			else:
				os.system("clear")
			os.system('clear')
			if last_ts:
				pp('%20s\t%10s\t%10s' % (columns[0], columns[1], "%s/sec" % columns[1]))
				#pp('%20s\t%10s\t%10s' % ("-" * 20, "-" * 10, "-" * 10))
			else:
				pp('%20s\t%10s' % (columns[0], columns[1]))
				#pp('%20s\t%10s' % ("-" * 20, "-" * 10))

      dc = data_function()

			for i,c in dc:
        try:
          c = int(c)
          if i in last_sample and last_ts:
            rate = (c-last_sample[i])/(ts-last_ts)
            pp('%20s\t%10d\t%10.2f' % (i, c, rate))
            total_rate = total_rate + rate
            last_sample[i] = c
          else:
            last_sample[i] = c
            pp('%20s\t%10d' % (i, c))
          total = total + c
        except:
          pp('%20s\t%s' % (i, c))

			if last_ts:
				pp('%20s\t%10d\t%10.2f' % ('Total', total, total_rate))
			else:
				pp('%20s\t%10d' % ('Total', total))

			interval = time.time() - ts
			if interval < sleep_time:
				time.sleep(sleep_time - interval)
			last_ts = ts
			if soopretty:
				myscreen.refresh()
				curses.flash()
	except KeyboardInterrupt:
		sys.exit(0)
	finally:
		if soopretty:
			curses.resetty()

if __name__ == '__main__':
  if options.show_indices_status:
    indices_status()
    sys.exit(0)

  if options.show_indices_nodes:
    indices_nodes(indice_pattern)
    sys.exit()

  if options.show_indices_routing:
    indices_routing(indice_pattern)
    sys.exit()

  if options.show_unassigned:
    unassigned_shards(indice_pattern)
    sys.exit()

  if options.cluster_health:
    updater_panel(["Variable", "Value"], cluster_health)

  #updater_panel(["Index", "Documents"], index_documents)
  pprint(doc_counts(""))
