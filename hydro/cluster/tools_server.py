#!/usr/bin/env python3

import zmq
from hydro.shared import util
from hydro.cluster.tools import (
	restart_all,
	clear_anna
)

PORT = 5000

def main():
	context = zmq.Context()
	socket = context.socket(zmq.REP)
	socket.bind('tcp://*:%d' % (PORT))

	client, apps_client = util.init_k8s()

	print('Started Anna tools server...')

	# Wait for restart messages
	while True:
		message = socket.recv_string()
		if message == 'RESTART':
			print('Received RESTART, restarting memory nodes...')
			restart_all(client)
		elif message == 'CLEAR':
			print('Received CLEAR, clearing all memory nodes...')
			clear_anna(client)
		else:
			print('Unknown message {}'.format(message))
			continue
		socket.send_string('Success')


if __name__ == '__main__':
	main()