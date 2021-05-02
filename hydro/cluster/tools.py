#!/usr/bin/env python3

import os
import sys
import subprocess
import time
from hydro.shared import util

BASE_CONFIG_FILE = '../anna/conf/anna-local.yml'
POD_CONFIG_DIR = '/hydro/anna/conf/'

main_client, main_apps_client = util.init_k8s()

def main():
	args = sys.argv[1:]
	cmd = args[0]

	if cmd == 'send-conf':
		ip = args[1]
		if ip == 'all':
			send_conf_all()
		else:
			send_conf(ip)
	elif cmd == 'restart':
		ip = args[1]
		if ip == 'all':
			restart_all()
		else:
			restart(ip)

def send_conf_all(client=None):
	pod_ips = util.get_pod_ips(client, selector='role=memory', is_running=True)
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

def restart_all(client=None):
	pod_ips = util.get_pod_ips(client, selector='role=memory', is_running=True)
	for pod_ip in pod_ips:
		restart(pod_ip)

def restart(ip, client=None):
	client = client if client is not None else main_client

	pod = util.get_pod_from_ip(client, ip)
	pname = pod.metadata.name
	cname = pod.spec.containers[0].name
	kill_cmd = 'kubectl exec -it %s -c %s -- /sbin/killall5' % (pname, cname)
	subprocess.run(kill_cmd, shell=True)
	pod_ips = util.get_pod_ips(client, selector='role=memory', is_running=True)
	while ip not in pod_ips:
		pod_ips = util.get_pod_ips(client, selector='role='+kind, is_running=True)
	send_conf(ip)

	print('Restarted %s' %(ip))

	
if __name__ == '__main__':
	main()