import zmq
import time
import redis
import sys
import threading

# port = '5556'

# context = zmq.Context()
# socket = context.socket(zmq.REP)

# socket.bind(f'tcp://*:{port}')

# while True:
#     message = socket.recv_json()
#     print('Received request:', message)
#     time.sleep(1)
#     socket.send_json({
#         'message': f'Hello from {port}'.encode()
#     })


# class Channel:
#     def __init__(
#         self,
#         n_bits,
#         redis_ip='*',
#         redis_port=6379,
#         host_ip='*',
#         host_port=7777
#     ):
#         self.n_bits = n_bits
#         self.channel = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
#         self.context = zmq.Context()
#         self.host_ip = host_ip
#         self.host_port = host_port
#         self.socket = None
#         self.MAX_PROC = 2 ** n_bits

#     def bind(self):
#         self.socket = self.context.socket(zmq.REP)
#         self.socket.bind(f'tcp://{self.host_ip}:{self.host_port}')
#         print(f'Binded to {self.host_ip}:{self.host_port}')

#     def send_to(self, from_, to_set, message):
#         for i in to_set:
#             self.channel.rpush(f'{from_}-{i}', message)
#             print(f'Sending "{message}"" from <{from_}> to <{i}>')

#     def recv_from(self, receiver, sender_set, timeout=0):
#         names = [f'{i}-{receiver}' for i in sender_set]

#         message = self.channel.blpop(names, timeout)
#         if message:
#             print(f'Received "{message}" from <{sender_set}> to {receiver}')
#             return message

#     def recv_from_thread(self, req, sock):
#         self.recv_from(req['from'], req['to'])
#         msg = self.recv_from()
#         res = {'response': msg}
#         sock.send_json(res)

#     def run(self):
#         self.bind()

#         print(f'Server listening on port {self.host_port}')
#         while True:
#             req = self.socket.recv_json()
#             print('Received request:', req)
#             if req.get('type') and req['type'].lower() == 'send':
#                 self.send_to(req['from'], req['to'], req['message'])
#                 res = {'response': 'OK'}

#             if req.get('type') and req['type'].lower() == 'recv':
#                 threading.Thread(self.recv_from_thread,
#                                  target=(req, self.socket))


# c = Channel(n_bits=5)

# c.run()
