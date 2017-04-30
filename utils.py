#!/bin/bash
import requests
import json
from configuration import *
import time
#username = "admin"
#password = "admin"
#ambari_server = "ambariserver.mycluster.com"
#cluster_name = "hadoopcluster"

def get_cluster_address():
	return "http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name
def get_host_address(host):
	return get_cluster_address() + "/hosts/" + host
def get_host_component_address(host, component):
	return get_host_address(host) + "/host_components/" + component
def get_service_address(service):
	return get_cluster_address() + "/services/" + service
def get_service_component_address(service, component):
	return get_service_address(service) + "/components/" + component
def get_host_metrics_address(host, metric):
	return get_host_address(host) + "?fields=metrics/" + metric
	
def get_service_component_metrics(service, component):
	r = requests.get(get_service_component_address(service, component), auth=('admin','admin'))
	print( r.text )
def get_service_metrics(service):
	r = requests.get(get_service_address(service) + "/components", auth=('admin', 'admin'))
	#print( r.text )
	data = r.json()
s = requests.Session()

# 0% - 100%
def get_host_cpu_usage(host):
	r = requests.get(get_host_metrics_address(host, "cpu"), auth = (username, password))
	data = r.json()
	return 100 - data["metrics"]["cpu"]["cpu_idle"]
 
def get_host_mem_usage():
	pass

def get_host_disk_usage_and_free_space():
	pass

def get_yarn_rm_metrics():
	#"http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/services/YARN/components/RESOURCEMANAGER?fields=metrics/yarn/Queue"
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get(get_service_component_address("YARN","RESOURCEMANAGER") + "?fields=metrics/yarn/Queue",headers=header,auth = (username, password))
	assert r.status_code == 200
	#print( r.text )
	data = r.json()
	print("EROARE", data)
	return data["metrics"]["yarn"]["Queue"]["root"]

def get_nm_cpu_usage_from_queue(q):
	assert q != None
	return q["AllocatedVCores"]/float(q["AllocatedVCores"] + q["AvailableVCores"]) * 100

def get_nm_mem_usage_from_queue(q):
	assert q!= None
	return q["AllocatedMB"]/float(q["AllocatedMB"] + q["AvailableMB"]) * 100

#return started_count : installed_count
def get_nodemangers_status():
	s = requests.Session()
	r = s.get(get_service_component_address("YARN", "NODEMANAGER"), auth=('admin', 'admin'))
	data = r.json()
	return (data["ServiceComponentInfo"]["started_count"], data["ServiceComponentInfo"]["installed_count"])

def get_nodemanager_cpu_usage():
	s = requests.Session()
	r = s.get(get_service_component_address("YARN", "NODEMANAGER"), auth=('admin', 'admin'))
	hosts = r.json()
	cpu = 0.0
	cnt = 0.0
	for namenode in hosts["host_components"]:
		r = s.get(namenode["href"], auth=(username, password))
		data = r.json()
		cpu += data["metrics"]["cpu"]["cpu_idle"]
		cnt += 1
	s.close()
	return 100 - cpu/cnt

def get_decommissioning_datanodes():
	# "http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/services/HDFS/components/NAMENODE?fields=ServiceComponentInfo/DecomNodes"
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get(get_service_component_address("HDFS","NAMENODE")+ "?fields=ServiceComponentInfo/DecomNodes",headers=header,auth = (username, password))
	data = r.json()
	data2 = json.loads(data["ServiceComponentInfo"]["DecomNodes"])
	cnt = 0
	list = []
	for x in data2:
		d = x.split(":")
		list.append(d[0])
		cnt += 1
	return cnt, list
# curl -u admin:admin -H "X-Requested-By: ambari" X GET  "http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/services/HDFS/components/DATANODE?fields=host_components/HostRoles/desired_admin_state,host_components/HostRoles/state"

def get_datanodes_state():
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get(get_service_component_address("HDFS","DATANODE")
	+ "?fields=host_components/HostRoles/desired_admin_state,host_components/HostRoles/state",
	headers=header,
	auth=(username, password))
	if r.status_code != 200:
		return None
	data = r.json()
	decommissioned_hosts = []
	started_hosts = []
	for x in data["host_components"]:
		if x["HostRoles"]["desired_admin_state"] == "DECOMMISSIONED":
			decommissioned_hosts.append(x["HostRoles"]["host_name"])
		if x["HostRoles"]["desired_admin_state"] == "INSERVICE" and x["HostRoles"]["state"]== "STARTED":
			started_hosts.append(x["HostRoles"]["host_name"])
	return (decommissioned_hosts, started_hosts)

def get_decommissioned_datanodes():
	#curl -u admin:admin -H "X-Requested-By: ambari" -X GET  "http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/services/HDFS/components/DATANODE?host_components/HostRoles/desired_admin_state=DECOMMISSIONED"
	r = requests.get(get_service_component_address("HDFS", "DATANODE") + 
	"?host_components/HostRoles/desired_admin_state=DECOMMISSIONED", auth=('admin', 'admin'))
	#print( r.status_code )
	data = r.json()
	list = []
	for x in data["host_components"]:
		list.append(x["HostRoles"]["host_name"])
	return list

def get_nodemanagers_state():
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get(get_service_component_address("YARN","NODEMANAGER")
	+ "?fields=host_components/HostRoles/desired_admin_state,host_components/HostRoles/state",
	headers=header,
	auth=(username, password))
	if r.status_code != 200:
		return None
	data = r.json()
	decommissioned_hosts = []
	started_hosts = []
	for x in data["host_components"]:
		if x["HostRoles"]["desired_admin_state"] == "DECOMMISSIONED":
			decommissioned_hosts.append(x["HostRoles"]["host_name"])
		if x["HostRoles"]["desired_admin_state"] == "INSERVICE" and x["HostRoles"]["state"]== "STARTED":
			started_hosts.append(x["HostRoles"]["host_name"])
	print(data)
	return (decommissioned_hosts, started_hosts)

def get_free_hdfs_space_from_namenode():
	r = requests.get(get_service_component_address("HDFS", "NAMENODE"), auth=('admin', 'admin'))
	print( r.text )
	#print( r.headers
	data = r.json()
	#print( data["metrics"]["dfs"]["namenode"]["PercentRemaining"] )
	r.close()
	return data["metrics"]["dfs"]["namenode"]["PercentRemaining"]

#minimize time using partial request
def get_free_hdfs_space_from_namenode_pr():
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get(get_service_component_address("HDFS", "NAMENODE")+"?fields=metrics/dfs/namenode/Free,metrics/dfs/namenode/PercentRemaining",headers=header,auth=(username, password))
	#print( r.text )
	#print( r.headers )
	data = r.json()
	#print( data)
	r.close()
	return (100 - data["metrics"]["dfs"]["namenode"]["PercentRemaining"],data["metrics"]["dfs"]["namenode"]["Free"]/(1024*1024*1024))


def get_free_hdfs_space():
	r = requests.get(get_service_component_address("HDFS", "DATANODE"), auth=('admin', 'admin'))
	data = r.json()
	datanode_href = []
	for d in data['host_components']:
		datanode_href.append(d['href'])
	cnt = 0
	total_capacity = 0.0
	free_space  = 0.0
	for href in datanode_href:
		cnt += 1
		r = requests.get(href, auth=('admin','admin'))
		data = r.json()
		total_capacity += data['metrics']['dfs']['FSDatasetState']['Capacity']
		volumeinfo =  json.loads(data['metrics']['dfs']['FSNamesystem']['VolumeInfo'])
		#print( volumeinfo )
		free_space += volumeinfo['/hadoop/hdfs/data/current']['freeSpace']
	print( "Capacity used :" + str(1 - free_space/total_capacity) )
	print( "Capacity free :" + str(free_space/total_capacity) )
	
	return free_space/total_capacity	


def add_component(component, host):
	# POST clusters/:clusterName/hosts/:hostName/host_components/:componentName
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.post("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name + "/hosts/" +  host + "/host_components/" + component ,headers=header,auth = (username, password))

	print( r.status_code )

def add_and_start_component(component, host):
	# component is in init state
	add_component(component,host)
	# component is in installed state
	stop_component(component, host)
	# component is in started state
	start_component(component, host)
	
def uninstall_component(component,host):
	header = {}
	header["X-Requested-By"] = "ambari"
	print( header )
	mydata = json.dumps({"HostRoles": {"desired_admin_state": "DECOMMISSIONED"}})
	print( mydata )
	r = requests.put("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name + "/hosts/" +  host + "/host_components/" + component ,data=mydata,headers=header,auth = (username, password))
	print(r.status_code)    
	print("done")

def get_running_yarn_applications_pr():
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get(get_service_component_address("YARN", "RESOURCEMANAGER") + "?fields=metrics/yarn/Queue/root/AppsRunning",headers = header,auth=(username, password))
	assert r.status_code == 200
	data = r.json()
	#print( data )
	#Original: return data["metrics"]["yarn"]["Queue"]["root"]["AppsRunning"] -- e un integer
	return 0

def get_running_yarn_applications():
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name + "/services/YARN/components/RESOURCEMANAGER",auth = (username, password))
	#print( r.status_code )
	data = r.json()
	metrics = data["metrics"]["yarn"]["Queue"]
	running_apps = 0
	for k in metrics.keys():
		running_apps +=  metrics[k]["AppsRunning"]
	return running_apps

def get_running_applications():
	return get_running_yarn_applications()

def delete_component(component, host):
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.delete("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name + "/hosts/" +  host + "/host_components/" + component ,headers=header,auth = (username, password))
	print( r.status_code )

def delete_host(host):
	header = {}
	header["X-Requested-By"] = "ambari"
	print( "http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name +  "/hosts/" +  host )
	r = requests.delete("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name +  "/hosts/" +  host,headers=header,auth = (username, password))
	return r.status_code

def add_host(host):
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.post("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name + "/hosts/" +  host,headers=header,auth = (username, password))
	return r.status_code

def get_all_healthy_registered_unattached_hosts():
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get("http://" + ambari_server + ":8080/api/v1" + "/hosts"+ "?Hosts/host_state=HEALTHY",auth = (username, password))
	data = r.json()
	l = []
	for x in data["items"]:
		if 'cluster_name' not in x["Hosts"]:
			l.append(x["Hosts"]["host_name"])
	print( l )
	return l
	
def verify_host_is_healthy_and_registered(host):
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get("http://" + ambari_server + ":8080/api/v1"+ "/hosts/" +  host,auth = (username, password))
	if r.status_code == 200:
		data = r.json()
		return data['Hosts']['host_state'] == 'HEALTHY'
	return False

def verify_host_is_added(host):
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name + "/hosts/" +  host,auth = (username, password))
	if r.status_code == 200:
		data = r.json()
		return data['Hosts']['host_state'] == 'HEALTHY'
	return False
    #return r.status_code == 200


#done
def stop_component(component, host):
	header = {}
	header["X-Requested-By"] = "ambari"
	#print( header )
	mydata = json.dumps({"HostRoles": {"state": "INSTALLED"}})
	#print( mydata )
	r = requests.put("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name + "/hosts/" +  host + "/host_components/" + component ,data=mydata,        headers=header,        auth = (username, password))
	#print( r.status_code )
	#print( r.headers )
	if r.status_code != 202:
		return None
	data = r.json()
	return data["href"]

def stop_and_delete_component(component, host):
	# installed state
	stop_component(component, host)
	# delete component
	delete_component(component, host)


#done
def start_component(component, host):
	# curl -u admin:$PASSWORD -i -H 'X-Requested-By: ambari' -X PUT -d '{"HostRoles": {"state": "INSTALLED/STARTED"}}' http://AMBARI_SERVER_HOST:8080/api/v1/clusters/CLUSTER_NAME/hosts/NEW_HOST_ADDED/host_components/DATANODE
	header = {}
	header["X-Requested-By"] = "ambari"
	print( header )
	mydata = json.dumps({"HostRoles": {"state": "STARTED"}})
	print( mydata )
	r = requests.put("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name + "/hosts/" +  host + "/host_components/" + component ,
	data=mydata,
	headers=header,
	auth = (username, password))
	print( r.status_code )
	print( r.text )
	if r.status_code != 202:
		return "none"
	#print( r.text )
	data = r.json()
	return data["href"]
	

#done
def list_all_components(host):
	d = {}
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get("http://" + ambari_server + ":8080/api/v1/clusters/" + cluster_name + "/hosts/" +  host + "/host_components",auth = (username, password))
	data = r.json()
	for t in data["items"]:
		print( t["HostRoles"]["component_name"] )
		rp = requests.get(t["href"], auth = (username, password))
		d[t["HostRoles"]["component_name"]] = rp.json()
		print( d[t["HostRoles"]["component_name"]]["HostRoles"]["state"] )
		#print( rp.text )
	# return a dictionary component - component information
	return d

def list_requests():
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get(get_cluster_address() + "/requests",	auth = (username, password))
	print( r.text )
	return r

def list_workflows():
	header = {}
	header["X-Requested-By"] = "ambari"
	r = requests.get(get_cluster_address() + "/workflows",        auth = (username, password))
	print( r.text )
	return r

def recomission_nodemanger_from_hosts(hosts):
	header = {}
	header["X-Requested-By"] = "ambari"
	mydata = json.dumps({
   "RequestInfo":{
      "context":"Recommission NodeManagers",
      "command":"DECOMMISSION",
      "parameters":{
         "slave_type":"NODEMANAGER",
         "included_hosts":hosts
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
	})
	r = requests.post(get_cluster_address() + "/requests" ,
        data=mydata,
        headers=header,
        auth = (username, password))
        #print( r.status_code )
	#print( r.text )
	if r.status_code != 202:
		return None
	data = r.json()
	return data["href"]

# returns True if all the ops are completed
def verify_requests(req_list):
	len_req = len(req_list)
	if len_req == 0:
		return True, req_list
	list = []
	response = True
	header = {}
	header["X-Requested-By"] = "ambari"
	ind = 0
	for x in range(len_req):
		r = requests.get(req_list[x], headers=header, auth=(username, password))
		data = r.json()
		if data["Requests"]["request_status"] != "COMPLETED":
			response = False
			#print( x )
			ind = x
			break
	if response == False:
		#print( ind )
		del req_list[:ind+1]
	else:
		del req_list[:]
	return response, req_list

def decomission_nodemanger_from_hosts(hosts):
	header = {}
	header["X-Requested-By"] = "ambari"
	mydata = json.dumps({
   "RequestInfo":{
      "context":"Decommission NodeManagers",
      "command":"DECOMMISSION",
      "parameters":{
         "slave_type":"NODEMANAGER",
         "excluded_hosts":hosts
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
	})
	r = requests.post(get_cluster_address() + "/requests" ,
        data=mydata,
        headers=header,
        auth = (username, password))
        #print( r.status_code )
	#print( r.text )
	if r.status_code != 202:
		return None
	data = r.json()
	return data["href"]

def decomission_datanodes_from_hosts(hosts):
	header = {}
	header["X-Requested-By"] = "ambari"
	mydata = json.dumps({
   "RequestInfo":{
      "context":"Decommission DataNodes",
      "command":"DECOMMISSION",
      "parameters":{
         "slave_type":"DATANODE",
         "excluded_hosts":hosts
      },
      "operation_level":{
         "level":"HOST_COMPONENT",
         "cluster_name":"hadoopcluster"
      }
   },
   "Requests/resource_filters":[
      {
         "service_name":"HDFS",
         "component_name":"NAMENODE"
      }
   ]
	})
	r = requests.post(get_cluster_address() + "/requests" ,
        data=mydata,
        headers=header,
        auth = (username, password))
        #print( r.status_code )
	#print( r.text )
	if r.status_code != 202:
		return None
	data = r.json()
	return data["href"]

def recomission_datanodes_from_hosts(hosts):
	header = {}
	header["X-Requested-By"] = "ambari"
	mydata = json.dumps({
   "RequestInfo":{
      "context":"Recommission DataNodes",
      "command":"DECOMMISSION",
      "parameters":{
         "slave_type":"DATANODE",
         "included_hosts":hosts
      },
      "operation_level":{
         "level":"HOST_COMPONENT",
         "cluster_name":"hadoopcluster"
      }
   },
   "Requests/resource_filters":[
      {
         "service_name":"HDFS",
         "component_name":"NAMENODE"
      }
   ]
	})
	r = requests.post(get_cluster_address() + "/requests" ,
        data=mydata,
        headers=header,
        auth = (username, password))
        #print( r.status_code )
	#print( r.text )
	if r.status_code != 202:
		return None
	data = r.json()
	return data["href"]



if __name__ == "__main__":
	q = None
	#print( decomission_datanodes_from_hosts("yarnmanagement.mycluster.com")
	print( username )
	print( password )
	print( ambari_server )
	print( cluster_name )
	print( get_running_yarn_applications_pr() )
	#return
	list = []
	list.append("http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/requests/249")
	list.append("http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/requests/199")
	list.append("http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/requests/162")
	#print( get_nodemanagers_state() )
	#print( get_decommissioning_datanodes() )
	#href1 = recomission_nodemanger_from_hosts("yarn.mycluster.com")
	#href2 =  start_component("NODEMANAGER", "yarnmanagement.mycluster.com")
	#print( href2 )
	list = []
	#list.append(href1)
	#list.append(href2)
	#print( list )
	#q = get_yarn_rm_metrics()
	return_datanodes_to_initial_state = False
	if return_datanodes_to_initial_state == True:
		decomission_datanodes_from_hosts("yarnmanagement.mycluster.com")
		decomission_datanodes_from_hosts("datanode4.mycluster.com")
		decomission_datanodes_from_hosts("namenode.mycluster.com")
	return_nodemangers_to_initial_state = False
	if return_nodemangers_to_initial_state == True:
		decomission_nodemanger_from_hosts("ambariserver.mycluster.com,namenode.mycluster.com,datanode4.mycluster.com,snodemgr.mycluster.com,yarn.mycluster.com,yarnmanagement.mycluster.com")
	response = True
	while response == False:
		import time
		response, list = verify_requests(list)
		print( response )
		time.sleep(1)
	while response == False:
		q = get_yarn_rm_metrics()
		print( q["AllocatedMB"] )
		print( q["AvailableMB"]	 )
		print( q["AvailableVCores"] )
		print( q["AllocatedVCores"] )
		print( "CPU usage" )
		print( get_nm_cpu_usage_from_queue(q) )
		print( "MEM usage" )
		print( get_nm_mem_usage_from_queue(q) )
		time.sleep(1)
	#response, list =  verify_requests(list)
	#print( response )
	#print( list )
	#print( username )
	#print( password )
	#add_component("NODEMANAGER","datanode4.mycluster.com")
	#stop_component("DATANODE","datanode4.mycluster.com")
	#add_host("datanode4.mycluster.com")
	#print( delete_host("datanode4.mycluster.com")
	#delete_component("METRICS_MONITOR", "datanode4.mycluster.com")
	#print( get_service_component_address("HDFS", "DATANODE") )
	#print( get_datanodes_state() )
	#print( get_nodemangers_status() )
	#print( get_host_cpu_usage("yarn.mycluster.com") )
	#print( get_free_hdfs_space_from_namenode_pr() )
	#print( get_free_hdfs_space() )
	#get_all_healthy_registered_unattached_hosts() )
	#print( delete_host("yarn.mycluster.com") )
	print( get_running_yarn_applications_pr())
	#add_and_start_component("DATANODE", "yarn.mycluster.com")
	#print( add_host("yarn.mycluster.com") )
	#print( list_all_components("datanode4.mycluster.com") )
	#print( verify_host_is_healthy_and_registered("yarn.mycluster.com") )
	#print( verify_host_is_registered("snsdfsdf") )
	# start_component("NODEMANAGER", "yarn.mycluster.com")
	#delete_component("NODEMANAGER", "yarnmanagement.mycluster.com")
	#list_all_components("datanode.mycluster.com")
	#get_free_hdfs_space()
	#list_workflows()
	#decomission_nodemanger_from_hosts("namenode.mycluster.com")
	#decomission_datanodes_from_hosts("namenode.mycluster.com")
	#stop_component("DATANODE", "datanode.mycluster.com")