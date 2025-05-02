# Author: Joseph Thalman
# This program sends mock commands to the manager program, acting as a Peer
# Displays messages sent and received between Peer and Manager
# n value for setup-dht is random and may cause FAILUREs if not enough peers are registered
# m_port and p_port are random and may cause FAILUREs if ports are already in use

import socket
import json
import random

# Manager info
manager_ip = 'localhost'
manager_port = int(input("Enter manager port: "))
manager_address = (manager_ip, manager_port)

# Peer socket setup
peer_p_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
peer_m_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Peer setup
peer_name = 'peer' + str(random.randint(1, 500))
peer_ip = 'localhost'
peer_p_port = random.randint(10500, 10999)
peer_m_port = random.randint(10500, 10999)
while peer_p_port == peer_m_port: # ensure ports are different
    peer_m_port = random.randint(10500, 10999)
peer = (peer_name, peer_ip, peer_m_port, peer_p_port)
print('Peer created: ', peer)

peer_p_socket.bind((peer_ip, peer_p_port))
peer_m_socket.bind((peer_ip, peer_m_port))


# Construct register command
def register(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1],
            'IPv4-address': param[2],
            'm-port': param[3],
            'p-port': param[4]
        }
    }

    return json.dumps(message).encode()

# Construct setup-dht command
def setup_dht(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1],
            'n': param[2],
            'YYYY': param[3]
        }
    }

    return json.dumps(message).encode()

# Construct dht-complete command
def dht_complete(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1]
        }
    }

    return json.dumps(message).encode()

def query_dht(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1]
        }
    }

    return json.dumps(message).encode()

def leave_dht(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1]
        }
    }

    return json.dumps(message).encode()

def join_dht(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1]
        }
    }

    return json.dumps(message).encode()

def dht_rebuilt(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1],
            'new-leader': param[2]
        }
    }

    return json.dumps(message).encode()

def deregister(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1]
        }
    }

    return json.dumps(message).encode()

def teardown_dht(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1]
        }
    }

    return json.dumps(message).encode()

def teardown_complete(param):
    message = {
        'command': param[0],
        'parameters': {
            'peer-name': param[1]
        }
    }

    return json.dumps(message).encode()

# Continuously prompt for new commands from peer
# 'exit' exits the program
while True:
     
    command = input('Command: ')

    param = command.split()

    message = ''

    match param[0]:
        case 'register':
            message = register(param)
        case 'setup-dht':
            message = setup_dht(param)
        case 'dht-complete':
            message = dht_complete(param)
        case 'query-dht':
            message = query_dht(param)
        case 'leave-dht':
            message = leave_dht(param)
        case 'join-dht':
            message = join_dht(param)
        case 'dht-rebuilt':
            message = dht_rebuilt(param)
        case 'deregister':
            message = deregister(param)
        case 'teardown-dht':
            message = teardown_dht(param)
        case 'teardown-complete':
            message = teardown_complete(param)
        case 'exit':
            break
        case _:
            print('Invalid command:', message)

    if message:
        print('Sending: ', message.decode())
        peer_m_socket.sendto(message, manager_address)
        data, addr = peer_m_socket.recvfrom(4096)
        print('Manager: ', data.decode())

