import socket
import json
import os
import time
import threading
import math
from raft import RaftNode
from constants import *
from termcolor import colored

# Create messenge request for request vote RPC
def create_msg_request_vote(sender, request, currentTerm, key="", value="", lastLogIndex=0, lastLogTerm=0):
    msg = {
        "sender_name": sender,
        "request": request,
        "term": currentTerm,
        "key": key,
        "value": value,
        "candidateId": sender,
        "lastLogIndex": lastLogIndex,
        "lastLogTerm": lastLogTerm
    }
    msg_bytes = json.dumps(msg).encode()
    return msg_bytes

# Create messenge request fot append entry RPC
def create_msg_append_entry(sender, request, currentTerm, key="", value="", entries=[], prevLogIndex=0, prevLogTerm=0):
    msg = {
        "sender_name": sender,
        "request": request,
        "term": currentTerm,
        "key": key,
        "value": value,
        "leaderId": sender,
        "entries": entries,
        "prevLogIndex": prevLogIndex,
        "prevLogTerm": prevLogTerm
    }
    msg_bytes = json.dumps(msg).encode()
    return msg_bytes

# Create message request
def create_msg(sender, request, currentTerm, key="", value=""):
    msg = {
        "sender_name": sender,
        "request": request,
        "term": currentTerm,
        "key": key,
        "value": value
    }
    msg_bytes = json.dumps(msg).encode()
    return msg_bytes

# Receive vote requests from all nodes and cast a vote
def vote_request(skt, node: RaftNode, self_node, target, term): 
    if node.currentTerm < term:
        node.currentTerm = term
        node.state = FOLLOWER
        node.votedFor = None

    if node.votedFor == None:
        node.startTime = time.perf_counter()
        node.electionTimeout = node.getElectionTimeout()
        node.votedFor = target
        msg_bytes = create_msg(self_node, VOTE_ACK, node.currentTerm)
        skt.sendto(msg_bytes, (target, 5555))

# Check for majority of votes and convert itself to a Leader
def vote_ack(node: RaftNode, nodes, self_node):
    node.voteCount += 1
    if node.voteCount >= math.ceil((len(nodes)+1)/2.0):
        node.heartbeatTimeout = node.getHeartbeatTimeout()
        node.startTime = time.perf_counter()
        node.state = LEADER
        node.currentLeader = self_node

# Receive heartbeats from leader node and reset election timeout
def append_rpc(node: RaftNode, term, leader):
    node.startTime = time.perf_counter()
    node.electionTimeout = node.getElectionTimeout()
    node.currentLeader = leader
    node.currentTerm = term
    if node.state == CANDIDATE and term >= node.currentTerm:
        node.currentTerm = term
        node.state = FOLLOWER

# Convert a node to follower state
def convert_follower(node: RaftNode):
    node.state = FOLLOWER
    node.votedFor = None
    node.voteCount = 0
    node.shutdown = False
    node.startTime = time.perf_counter()
    node.electionTimeout = node.getElectionTimeout()

# Timeout a node immediately
def timeout(node: RaftNode):
    node.state = 'FOLLOWER'
    node.startTime = time.perf_counter()
    node.electionTimeout = 0

# Send leader information to the controller
def leader_info(skt, node: RaftNode, self_node):
    msg_bytes = create_msg(
        self_node, LEADER_INFO, node.currentTerm, "LEADER", node.currentLeader)
    skt.sendto(msg_bytes, ('Controller', 5555))


# Listen for incoming requests
def listener(skt, node: RaftNode, nodes, self_node):
    while True:
        msg, addr = skt.recvfrom(1024)
        decoded_msg = json.loads(msg.decode('utf-8'))

        if not node.shutdown or decoded_msg['request'] == CONVERT_FOLLOWER:
            print(f"Message Received : {decoded_msg} From : {addr}")

            if decoded_msg['request'] == VOTE_REQUEST:
                vote_request(
                    skt, node, self_node, decoded_msg['sender_name'], decoded_msg['term'])

            elif decoded_msg['request'] == VOTE_ACK:
                vote_ack(node, nodes, self_node)

            elif decoded_msg['request'] == APPEND_RPC:
                append_rpc(node, decoded_msg['term'],
                           decoded_msg['sender_name'])

            elif decoded_msg['request'] == CONVERT_FOLLOWER:
                print(colored('          ************************   Converting ' + self_node +
                      ' To Follower   ************************', 'yellow', attrs=['bold']))
                convert_follower(node)

            elif decoded_msg['request'] == TIMEOUT:
                print(colored('          ************************   Timing Out ' +
                      self_node + '   ************************', 'red', attrs=['bold']))
                timeout(node)

            elif decoded_msg['request'] == SHUTDOWN:
                print(colored('          ************************   Shutting Down ' +
                      self_node + '   ************************', 'red', attrs=['bold']))
                node.shutdown = True

            elif decoded_msg['request'] == LEADER_INFO:
                leader_info(skt, node, self_node)

# Sends RPCs
def messenger(skt, node: RaftNode, sender, target):
    while(True):
        if not node.shutdown:
            if node.state == LEADER:
                if (node.startTime + node.heartbeatTimeout) < time.perf_counter():
                    node.heartbeatTimeout = node.getHeartbeatTimeout()
                    node.startTime = time.perf_counter()
                    for target in targets:
                        msg_bytes = create_msg_append_entry(
                            sender, APPEND_RPC, node.currentTerm)
                        skt.sendto(msg_bytes, (target, 5555))

            if node.state == FOLLOWER:
                if (node.startTime + node.electionTimeout) < time.perf_counter():
                    print(colored(
                        '          ************************   Starting Elections  ************************', 'green', attrs=['bold']))
                    node.state = CANDIDATE
                    node.currentTerm += 1
                    node.votedFor = self_node
                    node.voteCount = 1

                    for target in targets:
                        msg_bytes = create_msg_request_vote(
                            sender, VOTE_REQUEST, node.currentTerm)
                        skt.sendto(msg_bytes, (target, 5555))


if __name__ == "__main__":

    self_node = os.getenv('NODE_NAME')
    sender = self_node
    nodes = ["Node1", "Node2", "Node3", "Node4", "Node5"]
    targets = nodes
    targets.remove(self_node)
    node = RaftNode()

    UDP_Socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDP_Socket.bind((sender, 5555))

    threading.Thread(target=listener, args=[
                     UDP_Socket, node, nodes, self_node]).start()

    threading.Thread(target=messenger, args=[
        UDP_Socket, node, sender, targets]).start()
