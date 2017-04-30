#!/bin/bash
#time curl -u admin:admin -H "X-Requested-By: ambari" -X GET  "http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/services/HDFS/components/DATANODE?fields=host_components/HostRoles/desired_admin_state,host_components/HostRoles/state"

#get nodes with decommission in progress
#time curl -u admin:admin -H "X-Requested-By: ambari" -X GET  "http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/services/HDFS/components/NAMENODE?fields=ServiceComponentInfo/LiveNodes"

#get free space and percent remaining
#curl -u admin:admin -H "X-Requested-By: ambari" -X GET  "http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/services/HDFS/components/NAMENODE?fields=metrics/dfs/namenode/PercentRemaining,metrics/dfs/namenode/Free"
# decomission  COMMAND decomission - use excluded hosts
# recomission COMMAND decomission - use included_hosts + must start node
curl -u admin:admin -i -H 'X-Requested-By: ambari' -X POST -d '{
   "RequestInfo":{
      "context":"Recommission NodeManagers",
      "command":"DECOMMISSION",
      "parameters":{
         "slave_type":"NODEMANAGER",
         "included_hosts":"yarnmanagement.mycluster.com"
      },
      "operation_level":{
         "level":"HOST_COMPONENT",
         "cluster_name":"hadoopcluster"
      }
   },
   "Requests/resource_filters":[
      {
         "service_name":"YARN",
         "component_name":"RESOURCEMANAGER"
      }
   ]
}' http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/requests
