import math
import os
import pickle
import random

import redis


class Channel():
    def __init__(self, n_bits=5, host_ip='redis', port=6379):
        self.channel = redis.StrictRedis(host=host_ip, port=port, db=0)
        self.osmembers = {}
        self.n_bits = n_bits
        self.MAXPROC = 2 ** n_bits

    def join(self, subgroup):
        members = self.channel.smembers('members')
        newpid = random.choice(
            list(set([str(i) for i in range(self.MAXPROC)]) - members))
        if len(members) > 0:
            xchan = [[str(newpid), other] for other in members] + \
                [[other, str(newpid)] for other in members]
            for xc in xchan:
                self.channel.rpush('xchan', pickle.dumps(xc))
        self.channel.sadd('members', str(newpid))
        self.channel.sadd(subgroup, str(newpid))
        return str(newpid)

    def leave(self, subgroup):
        ospid = os.getpid()
        pid = self.osmembers[ospid]
        assert self.channel.sismember('members', pid), f'{pid} is not a member'
        del self.osmembers[ospid]
        self.channel.sdel('members', str(pid))
        members = self.channel.smembers('members')
        if len(members) > 0:
            xchan = [[str(pid), other] for other in members] + \
                [[other, str(pid)] for other in members]
            for xc in xchan:
                self.channel.rpop('xchan', pickle.dumps(xc))
        self.channel.sdel(subgroup, str(pid))
        return

    def exists(self, pid):
        return self.channel.sismember('members', pid)

    def bind(self, pid):
        ospid = os.getpid()
        self.osmembers[ospid] = str(pid)

    def subgroup(self, subgroup):
        return list(self.channel.smembers(subgroup))

    def send_to(self, destination_set, message):
        caller = self.osmembers[os.getpid()]
        assert self.channel.sismember(
            'members', caller), f'send_to: {str(caller)} is not a member'
        for i in destination_set:
            assert self.channel.sismember(
                'members', i), f'send_to: {i} is not a member'
            print('message sent:', str(caller), i, message)
            self.channel.rpush([str(caller), i], pickle.dumps(message))
    
    def send_to_all(self, message):
        caller = self.osmembers[os.getpid()]
        assert self.channel.sismember('members', caller), ''
        for i in self.channel.smembers('members'):
            self.channel.rpush([str(caller), i], pickle.dumps(message))
    
    def recv_from(self, sender_set, timeout=0):
        caller = self.osmembers[os.getpid()]
        assert self.channel.sismember('members', caller), ''
        for i in sender_set:
            assert self.channel.sismember('members', i), ''
        xchan = [[i, str(caller)] for i in sender_set]
        msg = self.channel.blpop(xchan, timeout)
        if msg:
            return [msg[0].split("'")[1], pickle.loads(msg[1])]

    def recv_from_any(self, timeout=9):
        caller = self.osmembers[os.getpid()]
        assert self.channel.sismember('members', caller), ''
        members = self.channel.smembers('members')
        xchan = [[i, caller] for i in members]
        print('debug:', self.channel.lrange(caller, 0, -1))
        msg = self.channel.blpop(xchan, timeout)
        print('members', members)
        print('caller:', caller)
        print('XCHAN:', xchan)
        print('MSG is:', msg)
        if msg:
            return [msg[0].split("'")[1], pickle.loads(msg[1])]
