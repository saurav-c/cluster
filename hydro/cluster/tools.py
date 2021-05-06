#!/usr/bin/env python3

import os
import sys
import subprocess
import time
from hydro.shared import util

BASE_CONFIG_FILE = '../anna/conf/anna-base.yml'
POD_CONFIG_DIR = '/hydro/anna/conf/'

main_client, main_apps_client = util.init_k8s()

def main():
	args = sys.argv[1:]
	cmd = args[0]

	if cmd == 'send-conf':
		ip = args[1]
		if ip == 'all':
			kind = args[2] if len(args) > 2 else 'memory'
			send_conf_all(kind=kind)
		else:
			send_conf(ip)
	elif cmd == 'restart':
		ip = args[1]
		if ip == 'all':
			kind = args[2] if len(args) > 2 else 'memory'
			restart_all()
		else:
			restart(ip)
	elif cmd == 'clear':
		clear_anna()

def send_conf_all(client=None, kind='memory'):
	client = client if client is not None else main_client
	pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)
	for pod_ip in pod_ips:
		send_conf(pod_ip)


def send_conf(ip, client=None):
	client = client if client is not None else main_client
	os.system('cp %s ./anna-config.yml' % BASE_CONFIG_FILE)

	pod = util.get_pod_from_ip(client, ip)
	pname = pod.metadata.name
	cname = pod.spec.containers[0].name

	retry = 0
	while True:
		try:
			util.copy_file_to_pod(client, './anna-config.yml', pname,
	                                      POD_CONFIG_DIR, cname)
			print('Sent config to %s' %(ip))
			break
		except Exception as e:
			retry += 1
			if retry > 5:
				print('Out of retries...exiting')
				break
			print('Retrying in %d sec' % (retry * 10))
			time.sleep(retry * 10)

	os.system('rm ./anna-config.yml')

def restart_all(client=None, kind='memory'):
	client = client if client is not None else main_client
	pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)
	for pod_ip in pod_ips:
		restart(pod_ip, kind)

def restart(ip, client=None, kind='memory'):
	client = client if client is not None else main_client

	pod = util.get_pod_from_ip(client, ip)
	pname = pod.metadata.name
	cname = pod.spec.containers[0].name
	kill_cmd = 'kubectl exec -it %s -c %s -- /sbin/killall5' % (pname, cname)
	subprocess.run(kill_cmd, shell=True)
	pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)
	while ip not in pod_ips:
		pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)
	send_conf(ip)

	print('Restarted %s' %(ip))

def clear_anna(client=None):
	client = client if client is not None else main_client

	node_ips = util.get_node_ips(client, 'role=memory', 'ExternalIP')

	import sys
	sys.path.append('./../anna/client/python')

	from anna.client import AnnaTcpClient
	from anna.anna_pb2 import GET
	from anna.zmq_util import send_request

	c = AnnaTcpClient('', None)
	req, _ = c._prepare_data_request(['DELETE'])
	req.type = GET

	for ip in node_ips:
		for i in range(4):
			addr = 'tcp://' + ip + ':' + str(6200 + i)
			send_sock = c.pusher_cache.get(addr)

			send_request(req, send_sock)
			print('Cleared %s' % (addr))
		print('Cleared %s' % (ip))

	del c

if __name__ == '__main__':
	main()