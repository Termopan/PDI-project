username = admin
password = admin
# old one:server_host = "ambariserver.mycluster.com"
server_host = "c7002.ambari.apache.org"
#this will set minimum datanode number to 2
dfs_replication = 2
# old one:excluded_datanode_hosts = ["ambariserver.mycluster.com"]
excluded_datanode_hosts = ["c7002.ambari.apache.org"]
excluded_nodemanager_hosts = []
datanode_add_threshold = 80
datanode_lt_add_free_space = 1024*1024*1024*4
datanode_gt_add_free_space = 15*1024*1024*1024
datanode_remove_free_space = 30*1024*1024*1024
datanode_remove_treshhold = 60
nodemanager_add_threshold=95
nodemanager_remove_threshold=40
#wait 5 minutes before removing/decommission service slave components
remove_wait_time=5*60
#wait 1 minute before add/recommission service slave components
add_wait_time=60
