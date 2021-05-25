import channel
import random
import math
from const_chord import (
    ANNOUNCE,
    JOIN,
    LEAVE,
    LOOKUP_REP,
    LOOKUP_REQ,
    STOP
)


class ChordNode:
    def __init__(self, channel: channel.Channel):
        self.channel = channel
        self.n_bits = channel.n_bits
        self.MAXPROC = channel.MAXPROC
        self.node_id = int(self.channel.join('node'))
        self.FT = [None for i in range(self.n_bits + 1)]
        self.node_set = []

    def in_between(self, key, lo, hi):
        if lo <= hi:
            return lo <= key and key < hi
        else:
            return (lo <= key and key < hi + self.MAXPROC) or \
                (lo <= key + self.MAXPROC and key < hi)

    def add_node(self, node_id):
        self.node_set.append(int(node_id))
        self.node_set = list(set(self.node_set))
        self.node_set.sort()

    def del_node(self, node_id):
        if node_id not in self.node_set:
            raise Exception(f'Node {node_id} does not exists')

        del self.node_set[self.node_set.index(node_id)]
        self.node_set.sort()

    def finger(self, i):
        succ = (self.node_id + 2 ** (i - 1)) % self.MAXPROC
        lwbi = self.node_set.index(self.node_id)
        upbi = (lwbi + 1) % len(self.node_set)
        for k in range(len(self.node_set)):
            if self.in_between(succ, self.node_set[lwbi] + 1, self.node_set[upbi] + 1):
                return self.node_set[upbi]
            (lwbi, upbi) = (upbi, (upbi + 1) % len(self.node_set))
        return None

    def recompute_finger_table(self):
        self.FT[0] = self.node_set[self.node_set.index(self.node_id) - 1]
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
        self.channel.bind(self.node_id)
        self.add_node(self.node_id)
        others = list(self.channel.channel.smembers('node') -
                      set([str(self.node_id)]))

        for i in others:
            self.add_node(i)
            self.channel.send_to([i], (JOIN))
        self.recompute_finger_table()

        while True:
            message = self.channel.recv_from_any()
            sender = message[0]
            request = message[1]
            if request[0] != LEAVE and \
                    self.channel.channel.sismember('node', str(sender)):
                self.add_node(sender)
            if request[0] == STOP:
                break
            if request[0] == LOOKUP_REQ:
                next_id = self.local_succ_node(request[1])
                self.channel.send_to([sender], (LOOKUP_REP, next_id))
                if not self.channel.exists(next_id):
                    self.del_node(next_id)
            elif request[0] == JOIN:
                continue
            elif request[0] == LEAVE:
                self.del_node(sender)
            self.recompute_finger_table()
        print('FT[','%04d'%self.nodeID,']: ',['%04d' % k for k in self.FT]) #-


class ChordClient:
    def __init__(self, channel: channel.Channel):
        self.channel = channel
        self.node_id = int(self.channel.join('client'))
    
    def run(self):
        self.channel.bind(self.node_id)
        procs = [int(i) for i in list(self.channel.channel.smembers('node'))]
        procs.sort()
        print(['%04d' % k for k in procs]) #-

        p = procs[random.randint(0, len(procs) - 1)]
        key = random.randint(0, self.channel.MAXPROC - 1)
        print(self.node_id, 'sending LOOKUP request for', key, 'to', p)

        self.channel.send_to([p], (LOOKUP_REQ, key))
        msg = self.channel.recv_from([p])
        while msg[1][1] != p:
            p = msg[1][1]
            self.channel.send_to([p], (LOOKUP_REQ, key))
            msg = self.channel.recv_from([p])
        print(self.node_id, 'received final answer from', p)
        self.channel.send_to(procs, (STOP))
