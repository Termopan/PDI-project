import subprocess, requests
#ret = subprocess.call(["ssh", "marius.eseanu@fep.grid.pub.ro"]);
#ret = subprocess.call(["ls", "-ahl"])

#r = requests.get('http://ambariserver.mycluster.com:8080/api/v1/clusters', auth=('admin', 'admin'))
#print r.headers['content-type']
#print "---"
#print  r.encoding
#print "---"
#print r.text
#print "---"
#print r.json()
#http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/services/HDFS/components/DATANODE
r = requests.get('http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/services/HDFS/components/DATANODE', auth=('admin', 'admin'))
#print r.text
data = r.json()
datanode_href = []
for d in data['host_components']:
	datanode_href.append(d['href'])
#print data['host_components']
#http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/hosts/datanode2.mycluster.com/host_components/DATANODE
#r = requests.get('http://ambariserver.mycluster.com:8080/api/v1/clusters/hadoopcluster/hosts/datanode2.mycluster.com/host_components/DATANODE', auth=('admin', 'admin'))
#print r.text
cnt = 0
for href in datanode_href:
	cnt += 1
	print "AAAAAAAAAAAAA %d" % cnt
	r = requests.get(href, auth=('admin','admin'))
	component = r.json()
	print component['metrics']

