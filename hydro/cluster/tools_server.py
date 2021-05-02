#!/usr/bin/env python3

import zmq
from hydro.shared import util
from tools import restart_all

PORT = 5000

def main():
	context = zmq.Context()
	socket = context.socket(zmq.REP)
	socket.bind('tcp://*:%d' % (PORT))

	client, apps_client = util.init_k8s()

	print('Started Anna tools server...')
	
	# Wait for restart messages
	while True:
		message = socket.recv()
		print('Received message, restarting memory nodes...')
		restart_all(client)
		socket.send('Success')


if __name__ == '__main__':
	main()