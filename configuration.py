#!/bin/python
dfs_replication = 2
username = "admin"
password = "admin"
ambari_server = "c7002.ambari.apache.org"
# old one: ambari_server = "ambariserver.mycluster.com"
cluster_name = "hadoopcluster"
# add datanode if pecentage of used space is threshold and has 5 Gb free
add_datanode_tr = 80
add_datanode_lt_fs = 4
add_datanode_gt_fs = 10
remove_datanode_fr = 30
add_nodemanager_cpu_tr = 80
# for spikes
remove_nodemanager_cpu_tr = 20
add_nodemanager_mem_tr = 80
remove_nodemanager_mem_tr = 20
# all strategies implies that len(nodemanagers)==len(datanodes) when no applications are running
#0 - less resources is the best strategy
#1 - threshold is taken into consideration
#2 - timing is important add all nodemanagers available then decommission them
# if they are no application is running
resources_allocation=1
remove_datanode_no_app = 20#20
