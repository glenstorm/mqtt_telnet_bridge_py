import socketserver
import paho.mqtt.client as mqtt


class MyTCPHandler(socketserver.BaseRequestHandler):
	def setup(self):
		self.start_poll=False
		self.broker_address="127.0.0.1"
		self.client = mqtt.Client()
		self.client.on_message=self.on_message
		self.client.connect(self.broker_address)
		print("Connect to mqtt: {0}".format(self.client_address))
		self.client.loop_start()

	def finish(self):
		print("Disconnect from mqtt: {0}".format(self.client_address))
		self.client.loop_stop()

	def handle(self):
		while True:
			# self.request is the TCP socket connected to the client
			try:
				self.data = self.request.recv(1024).strip().decode("utf-8")
				# print("input '{0}'".format(self.data))
			except Exception as e:
				break

			if self.start_poll:
				continue

			if self.data.startswith("subscribe "):
				topic = self.data.split()[1]
				print("subscribe '{0}' for client '{1}'".format(topic, self.client_address))
				self.client.subscribe(topic)
			elif self.data == "poll":
				print("poll for client '{0}'".format(self.client_address))
				self.start_poll = True

	def on_message(self, client, userdata, message):
		if self.start_poll:
			self.request.sendall("{0}: {1}\n".format(message.topic, message.payload.decode("utf-8")).encode())

if __name__ == "__main__":
	HOST, PORT = "localhost", 1234

	# Create the server, binding to localhost on port 1234
	with socketserver.ThreadingTCPServer((HOST, PORT), MyTCPHandler) as server:
		# Activate the server; this will keep running until you
		# interrupt the program with Ctrl-C
		try:
			server.serve_forever()
		except Exception as e:
			raise
		finally:
			server.shutdown()
