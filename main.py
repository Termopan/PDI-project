import time
from utils import *
import numpy as np
#matplotlib.use('GtkAgg')
import random
import credentials

def remove_smt(list):
	l = []
	for x in list:
		if x == "sdfas":
			# print( "DAAA" )
			l.append("adasdasd")
	del list[:]
	return True, l

if __name__=="__main__":
	ti = int(time.time())
	free_space = []
	used_space = []
	cpu_usage = []
	mem_usage = []
	len_cpu_usage = 0
	get_free_hdfs_space_from_namenode_pr()
	decommissioned_datanodes , started_datanodes = get_datanodes_state()
	decommissioned_nodemanagers, started_nodemanagers = get_nodemanagers_state()
	stopped_nodemanagers = []
	decommissioning_datanodes = []
	datanode_ops = []
	nodemanager_ops = []
	# add/remove
	last_datanode_change = "add"
	#print( decommissioned_datanodes, started_datanodes )
	#print( decommissioned_nodemanagers, started_nodemanagers )
	#datanode_ops.append("sdfas")
	#datanode_ops.append("adasdada")
	#print( "DAAAA" )
	#print( datanode_ops
	#value, datanode_ops = remove_smt(datanode_ops)
	#print( value )
	#print( datanode_ops )
	do_not_enter = True
	len_used_space = 0
	datanode_last_action_time = 0
	datanode_add_cooldown = 30
	datanode_remove_cooldown = 60
	nodemanager_last_action_time = 0
	nodemanager_add_cooldown = 50
	nodemanager_remove_cooldown = 60
	datanode_measurements = 10
	nodemanager_mesurements = 20 
	prev_datanode_ops_completed = True
	prev_nodemanager_ops_completed = True
	#nodemanager_add_ops_completed = True
	while True:
		datanode_ops_completed, datanode_ops = verify_requests(datanode_ops)
		nodemanager_ops_completed, nodemanager_ops = verify_requests(nodemanager_ops)
		ctime = int(time.time())
		#no_decommissioning_dn , decommissioning_datanodes = get_decommissioning_datanodes()
		running_applications = get_running_yarn_applications_pr()
		#print( "No of decommisioning datanodes" )
		#print( no_decommissioning_dn )
		print(  "Running applications ", running_applications)

		#print( "Datanode ops completed" )
		#print( datanode_ops_completed )
		#print( decommissioned_datanodes )
		#print( decommissioned_nodemanagers )
		print( "Started datanodes ", len(started_datanodes), " decom DN ",len(decommissioned_datanodes))
		print( "Started nodemanagers  ", len(started_nodemanagers),"  decom NM  ",len(decommissioned_nodemanagers))
		if do_not_enter == False and nodemanager_ops_completed == True and len(stopped_nodemanagers) !=  0:
			href = start_component("NODEMANAGER", stopped_nodemanagers.pop())
			#print( href  )
			nodemanager_ops.append(href)
			nodemanager_ops_completed = False
		
		if datanode_ops_completed == True and prev_datanode_ops_completed == False:
			datanode_last_action_time = ctime
		if nodemanager_ops_completed == True and prev_nodemanager_ops_completed == False:
			nodemanager_last_action_time = ctime

		
		prev_datanode_ops_completed = datanode_ops_completed
		prev_nodemanager_ops_completed = nodemanager_ops_completed
		
		if nodemanager_ops_completed == True and len(stopped_nodemanagers) !=  0:
			host = stopped_nodemanagers.pop()
			href = start_component("NODEMANAGER", host)
			print( href )
			if href != "none":
				nodemanager_ops.append(href)
				nodemanager_last_action_time = ctime
			else:
				stopped_component.append(host)
			print( "P2" )
			nodemanager_ops_completed = False
		

		# First wait to finish other operations
		if nodemanager_ops_completed == True and (len(started_nodemanagers) < len(started_datanodes)):
			dif = len(started_datanodes) - len(started_nodemanagers)
			cpu_usage = []
			mem_usage = []
			len_cpu_usage = 0
			nodemanager_ops_completed = False
			
			if dif > 0:
				host = decommissioned_nodemanagers.pop()
				href = recomission_nodemanger_from_hosts(host)
				if host != None:
					started_nodemanagers.append(host)
					stopped_nodemanagers.append(host)
					nodemanager_ops.append(href)
					nodemanager_last_action_time = ctime
				else:
					decommissioned_nodemanagers.append(host)
				#dif -= 1
				#nodemanager_add_ops_completed = False
				
		if nodemanager_ops_completed == True and running_applications == 0:
			dif = len(started_nodemanagers) - len(started_datanodes)
			if dif > 0:
				cpu_usage = []
				mem_usage = []
				len_cpu_usage = 0
				nodemanager_ops_completed = False
			if  dif > 0:
				host = started_nodemanagers.pop()
				href = decomission_nodemanger_from_hosts(host)
				if href != None:
					decommissioned_nodemanagers.append(host)
					nodemanager_ops.append(href)
					nodemanager_last_action_time = ctime
				else:
					started_nodemanagers.append(host)
		if running_applications > 0 and nodemanager_ops_completed == True and \
			resources_allocation == 2 and len(decommissioned_nodemanagers)!=0:
			nodemanager_ops_completed = False
			host = decommissioned_nodemanagers.pop()
			href = recomission_nodemanger_from_hosts(host)
			if host != None:
				stopped_nodemanagers.append(host)
				started_nodemanagers.append(host)
				nodemanager_last_action_time = ctime
			else:
				decommissioned_nodemanagers.append(host)
			#continue
		#print( "datanode last action time" )
		#print( ctime - datanode_last_action_time )
		#if datanode_ops_completed == False:
		#	time.sleep(1)
		#	continue
		#if nodemanager_ops_completed == False:
		#	time.sleep(1)
		#	continue
		
		if resources_allocation < 2 and  nodemanager_ops_completed == True:
			#print( "getting metrics" )
			q = get_yarn_rm_metrics()
			cpuu = get_nm_cpu_usage_from_queue(q)
			memu = get_nm_mem_usage_from_queue(q)
			cpu_usage.append(cpuu)
			mem_usage.append(memu)
			len_cpu_usage += 1
			
	
		if datanode_ops_completed == True:
			us, fs = get_free_hdfs_space_from_namenode_pr()
			used_space.append(us)
			free_space.append(fs)
			len_used_space += 1
			#time.sleep(1)
		#print( decommissioned_datanodes )
		#print( started_datanodes )
		#print( "" + i + " measurement"  )
		if datanode_ops_completed == True and len_used_space >= datanode_measurements:
			#print( "da" )
			if len_used_space > datanode_measurements:
				used_space = used_space[-datanode_measurements:]
				free_space = free_space[-datanode_measurements:]
				len_used_space = datanode_measurements
			free_space_avg = sum(free_space)/len_used_space
			used_space_avg = sum(used_space)/len_used_space
			#used_space = []
			#print( free_space )
			#print( used_space )
			#free_space = []
			#len_used_space = 0
			action = "none"
			if used_space_avg < add_datanode_tr and free_space_avg <= add_datanode_lt_fs:
				action = "add"
			if used_space_avg >= add_datanode_tr and free_space_avg <= add_datanode_gt_fs:
				action = "add"
			if free_space_avg > remove_datanode_fr:
				action = "remove"
			if free_space_avg >= remove_datanode_no_app and running_applications == 0:
				action = "remove"
			if action=="add" and (ctime - datanode_last_action_time) < datanode_add_cooldown:
				action = "none"
			if action == "add" and running_applications==0:
				action = "none"
			if action == "remove" and (ctime - datanode_last_action_time) < datanode_remove_cooldown:
				action = "none"
			
			no_decommissioning_dn = 0
			decommissioning_datanodes = []
			if action != "none":
				used_space = []
				free_space = []
				len_used_space = 0
				no_decommissioning_dn , decommissioning_datanodes = get_decommissioning_datanodes()
				print( "Decommissioning datanodes" +  no_decommissioning_dn )
			if action == "add":
				host = None
				if no_decommissioning_dn > 0:
					host = decommissioning_datanodes.pop()
					decommissioned_datanodes.remove(host)
				else:
					if len(decommissioned_datanodes) > 0:
						host = decommissioned_datanodes.pop()						
					else:
						host = None
				if host != None:
					started_datanodes.append(host)
					req = recomission_datanodes_from_hosts(host)
					datanode_ops.append(req)
					last_datanode_change = "add"
					cpu_usage = []
					mem_usage = []
					len_cpu_usage = 0
			#print( "Free space " + free_space_avg )
			#print( "Percentage used space " + used_space_avg )
			print( "Datanode Action" )
			print( action )
			print( "Datanode Action" )
			if action == "remove" and no_decommissioning_dn < (dfs_replication - 1):
				host = None
				if len(started_datanodes) > dfs_replication:
					host = started_datanodes.pop()
				else:
					host = None
				if host != None:
					decommissioned_datanodes.append(host)
					req = decomission_datanodes_from_hosts(host)
					datanode_ops.append(req)
					last_action = "remove"
					cpu_usage = []
					mem_usage = []
					len_cpu_usage = 0
		#print( "sleeping" )
		if resources_allocation < 2 and nodemanager_ops_completed == True and len_cpu_usage >= nodemanager_mesurements:
			#print( "DDDDDDDDDDAAAAAAAAAAAAAA" )
			if len_cpu_usage > nodemanager_mesurements:
				cpu_usage = cpu_usage[-nodemanager_mesurements:]
				mem_usage = mem_usage[-nodemanager_mesurements:]
				len_cpu_usage = nodemanager_mesurements
			cpu_usage_avg = sum(cpu_usage)/float(len_cpu_usage)
			mem_usage_avg = sum(mem_usage)/float(len_cpu_usage)
			#print( cpu_usage_avg )
			#print( mem_usage_avg )
			action = "none"
			print( resources_allocation )
			if resources_allocation == 1:
				if (mem_usage_avg > add_nodemanager_mem_tr) or (cpu_usage_avg >  add_nodemanager_cpu_tr):
					action = "add"
				if (mem_usage_avg < remove_nodemanager_mem_tr) and (cpu_usage_avg < add_nodemanager_cpu_tr):
					action = "remove"
			if resources_allocation == 0 and (cpu_usage_avg > 100 or mem_usage_avg > 100):
					action = "add"
			#print( "PHASE 1" )
			#print( action )
			if action == "add" and (ctime - nodemanager_last_action_time) < nodemanager_add_cooldown:
				action = "none"
			if action == "remove" and (ctime - nodemanager_last_action_time) < nodemanager_remove_cooldown:
				action = "none"
			if action == "remove" and len(started_nodemanagers)<= len(started_datanodes):
				action = "none"
			#print( "PHASE 2" )
			#print( action )
			if action == "add" and len(decommissioned_nodemanagers) == 0:
				action = "none"
			print( "NM ACTIONNN" )
			print( action )
			print( "NM action" )
			if action == "add":
				host = decommissioned_nodemanagers.pop()			
				started_nodemanagers.append(host)
				stopped_nodemanagers.append(host)
				href = recomission_nodemanger_from_hosts(host)
				nodemanager_ops.append(href)
				#nodemanager_add_ops_completed = False
			if action == "remove":
				host = started_nodemanagers.pop()
				decommissioned_nodemanagers.append(host)
				href = decomission_nodemanger_from_hosts(host)
				nodemanager_ops.append(href)
			if action != "none":
				cpu_usage = []
				mem_usage = []
				len_cpu_usage = 0
				nodemanager_last_action_time = ctime
				
				

			
		time.sleep(1)
		
		#free_space.append(tf-ti)
		#free_space = free_space[1:]
		#plt.plot(free_space)
		#plt.axis([0,100, 0,100])
		#plt.draw()
		#time.sleep(1)
		#plt.clf()
		
