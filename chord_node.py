import redis
import sys
import random
from redis import Redis
from const_chord import (
    JOIN,
    STOP,
    LEAVE,
    LOOKUP_REP,
    LOOKUP_REQ,
)


class Transport:
    def __init__(self, redis_ip='*', redis_port=6379, n_bits=5):
        self.channel = redis.StrictRedis(host=redis_ip, port=redis_port, db=0)
        self.n_bits = n_bits
        self.MAX_PROC = 2 ** n_bits
        self.node_id = None

    def join(self):
        members = self.channel.smembers('members')
        print('members already:', members)
        available = list(set([i for i in range(self.MAX_PROC)]) - members)
        new_id = random.choice(available)
        self.node_id = new_id
        self.channel.sadd('members', str(new_id))
        if len(members) > 0:
            # notify all other members
            notify_q = [f'{new_id}-{int(other)}' for other in members]
            for n in notify_q:
                self.channel.rpush(n, f'{JOIN}-""')
        return new_id

    def leave(self):
        self.channel.srem('members', str(self.node_id))
        members = self.channel.smembers('members')
        if len(members) > 0:
            # notify all other members
            notify_q = [f'{self.node_id}-{int(other)}' for other in members]
            for n in notify_q:
                self.channel.rpush(n, f'{LEAVE}-""')

    def send_to(self, destination_set, message):
        for i in destination_set:
            self.channel.rpush(f'{self.node_id}-{int(i)}', message)

    def send_to_all(self, message):
        members = self.channel.smembers('members')
        for i in members:
            self.channel.rpush(f'{self.node_id}-{int(i)}', message)

    def recv_from_any(self, timeout=0):
        members = self.channel.smembers('members')

        notify_q = [f'{int(other)}-{self.node_id}' for other in members]

        message = self.channel.blpop(notify_q, timeout=timeout)
        if message:
            a = message[0].decode().split('-')[0]
            b = message[1].decode().split('-')
            return a, b

        return None, (None, None)


class ChordNode:
    def __init__(self, redis_ip='*', redis_port=6379, n_bits=5):
        self.transport = Transport(
            redis_ip=redis_ip,
            redis_port=redis_port,
            n_bits=n_bits
        )
        self.n_bits = n_bits
        self.MAX_PROC = 2 ** n_bits
        self.node_id = None
        self.FT = [None for i in range(self.n_bits + 1)]
        self.node_set = []

    def add_node(self, node_id):
        self.node_set.append(int(node_id))
        self.node_set = list(set(self.node_set))
        self.node_set.sort()

    def del_node(self, node_id):
        print(self.node_set, node_id)
        if node_id not in self.node_set:
            raise Exception(f'Node {node_id} does not exists')

        del self.node_set[self.node_set.index(node_id)]
        self.node_set.sort()

    def in_between(self, key, lo, hi):
        if lo <= hi:
            return lo <= key and key < hi
        else:
            return (lo <= key and key < hi + self.MAX_PROC) or \
                (lo <= key + self.MAX_PROC and key < hi)

    def finger(self, i):
        succ = (self.node_id + 2 ** (i - 1)) % self.MAX_PROC
        lwbi = self.node_set.index(self.node_id)
        upbi = (lwbi + 1) % len(self.node_set)
        for k in range(len(self.node_set)):
            if self.in_between(succ, self.node_set[lwbi] + 1, self.node_set[upbi] + 1):
                return int(self.node_set[upbi])
            (lwbi, upbi) = (upbi, (upbi + 1) % len(self.node_set))
        return None

    def recompute_finger_table(self):
        self.FT[0] = int(self.node_set[self.node_set.index(self.node_id) - 1])
        self.FT[1:] = [self.finger(i) for i in range(1, self.n_bits + 1)]

    def local_succ_node(self, key):
        if self.in_between(key, self.FT[0] + 1, self.node_id + 1):
            return self.node_id
        elif self.in_between(key, self.node_id + 1, self.FT[1]):
            return self.FT[1]
        for i in range(1, self.n_bits + 1):
            if self.in_between(key, self.FT[i], self.FT[(i + 1) % self.n_bits]):
                return self.FT[i]

    def run(self):
        try:
            self.node_id = self.transport.join()
            self.add_node(self.node_id)
            print(f'node_id: {self.node_id}')

            others = list(self.transport.channel.smembers('members') - \
                        set([str(self.node_id)]))

            for i in others:
                if int(i) <= self.MAX_PROC:
                    self.add_node(int(i))
                    self.transport.send_to([i], f'{JOIN}-""')
            self.recompute_finger_table()

            print('node-set:', self.node_set)

            while True:
                sender, message = self.transport.recv_from_any(timeout=1)
                if message and message[0] is not None:
                    print('Message received from', sender, ':', message)
                if message[0] != LEAVE and sender is not None and \
                    self.transport.channel.sismember('members', int(sender)) and \
                    int(sender) <= self.MAX_PROC:
                    self.add_node(sender)
                    print(self.node_set)
                if message[0] == STOP:
                    break
                if message[0] == JOIN:
                    self.add_node(int(sender))
                    print(f'Added {sender}')
                    print(self.node_set)
                    continue
                if message[0] == LEAVE:
                    self.del_node(int(sender))
                    print(f'removed node {sender}')
                    print(self.node_set)
                if message[0] == LOOKUP_REQ:
                    next_id = int(self.local_succ_node(int(message[1])))
                    self.transport.send_to([sender], f'{LOOKUP_REP}-{next_id}')
                    print(f'Sent reply {LOOKUP_REP}-{next_id} to {sender}')
                    if not self.transport.channel.sismember('members', next_id):
                        self.del_node(next_id)
                self.recompute_finger_table()
        except KeyboardInterrupt:
            self.transport.leave()
            print('Interrupted by user...')
        print('FT[','%04d'%self.node_id,']: ',['%04d' % k for k in self.FT])


if __name__ == '__main__':
    m = 5
    if len(sys.argv) > 1:
        m = int(sys.argv[1])

    node = ChordNode(n_bits=m)
    node.run()
