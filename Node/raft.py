import time
import random


class RaftNode:
    def __init__(self, state="FOLLOWER", currentTerm=0, votedFor=None, log=[], voteCount=0):
        self.state = state
        self.currentTerm = currentTerm
        self.votedFor = votedFor
        self.log = log
        self.electionTimeout = self.getElectionTimeout()
        self.voteCount = voteCount
        self.startTime = time.perf_counter()
        self.currentLeader= ""
        self.shutdown = False
        self.heartbeatTimeout = self.getHeartbeatTimeout()


    def getElectionTimeout(self):
        return random.randint(500, 5000)/1000.0

    def getHeartbeatTimeout(self):
        return 0.5
