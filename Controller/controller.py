import json
import socket
import traceback
import time
import threading
import random

leader = ""
port = 5555
is_leader_set = False


# Get leader information
def leader_info(skt, nodes):
    request(skt, 'LEADER_INFO', nodes)

# Convert ALL nodes to follower state


def convert_all_to_follower(skt, nodes):
    request(skt, 'CONVERT_FOLLOWER', nodes)

# Convert a leader node to the follower state


def convert_leader_to_follower(skt, nodes):
    leader_info(skt, nodes)
    global is_leader_set
    while(not is_leader_set):
        continue
    is_leader_set = False
    request(skt, 'CONVERT_FOLLOWER', [leader])

# Shutdown any particular node


def shutdown_node(skt, nodes):
    shutdown_node = random.choice(nodes)
    request(skt, 'SHUTDOWN', [shutdown_node])


# Shutdown the leader node
def shutdown_leader(skt, nodes):
    leader_info(skt, nodes)
    global is_leader_set
    while(not is_leader_set):
        continue
    is_leader_set = False
    shutdown_node = leader
    request(skt, 'SHUTDOWN', [shutdown_node])

# Convert a node which has been shutdown


def convert_shutdown_node_to_follower(skt, nodes):
    shutdown_node = random.choice(nodes)
    request(skt, 'SHUTDOWN', [shutdown_node])
    time.sleep(2)
    request(skt, 'CONVERT_FOLLOWER', [shutdown_node])

# Timeout any particular node


def timeout_node(skt, nodes):
    timeout_node = random.choice(nodes)
    request(skt, 'TIMEOUT', [timeout_node])

# Timeout leader node


def timeout_leader(skt, nodes):
    leader_info(skt, nodes)
    global is_leader_set
    while(not is_leader_set):
        continue
    is_leader_set = False
    timeout_node = leader
    request(skt, 'TIMEOUT', [timeout_node])


# Create a message request
def create_msg(sender, request_type):
    msg = {
        "sender_name": sender,
        "request": request_type,
        "term": None,
        "key": "",
        "value": ""
    }
    msg_bytes = json.dumps(msg).encode()
    return msg_bytes


# Listen for leader information
def listener(skt):
    while True:
        msg, addr = skt.recvfrom(1024)
        decoded_msg = json.loads(msg.decode('utf-8'))
        global leader
        leader = decoded_msg['value']
        global is_leader_set
        is_leader_set = True
        print(f"Message Received : {decoded_msg} From : {addr}")

# Send controller requests


def request(skt, request_type, nodes):
    msg_bytes = create_msg('CONTROLLER', request_type)
    print(f"Request Created : {msg_bytes}")

    try:
        for target in nodes:
            skt.sendto(msg_bytes, (target, port))
    except:
        print(f"ERROR WHILE SENDING REQUEST ACROSS : {traceback.format_exc()}")


if __name__ == "__main__":
    time.sleep(5)
    sender = "Controller"
    nodes = ["Node1", "Node2", "Node3", "Node4", "Node5"]

    skt = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    skt.bind((sender, port))

    threading.Thread(target=listener, args=[skt]).start()
    run_test_case = True

    testCases = {
        1: leader_info,
        2: convert_all_to_follower,
        3: convert_leader_to_follower,
        4: shutdown_node,
        5: shutdown_leader,
        6: convert_shutdown_node_to_follower,
        7: timeout_node,
        8: timeout_leader,
    }

    # Run combination of different cases
    while run_test_case:
        select_test_case = random.randint(1, 8)
        testCases[select_test_case](skt, nodes)
        time.sleep(5)

    # Run any single case
    # testCases[3](skt, nodes)
