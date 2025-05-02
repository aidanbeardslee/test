# Author:  Joseph Thalman
# This program defines data structures, global variables, and constants 
#  used by the manager program.

import socket
from threading import Thread, Lock
import json
from enum import IntEnum
import logging
import traceback
from sys import exit

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# "Constants"
BUFFER_SIZE = 4096 # Max size of payload accepted

# Result messages to send peers
SUCCESS = {'status': 'SUCCESS'}
FAILURE = {'status': 'FAILURE'}
UNSUPPORTED = {'status': 'UNSUPPORTED'}

# Defines peer states as integer enums
class PeerState(IntEnum):
    FREE = 1    # Peer can partipate freely
    LEADER = 2  # Peer is leader of DHT construction
    IN_DHT = 3  # Peer is involved in DHT construction
    LEAVING = 4 # Peer is leaving DHT
    JOINING = 5 # Peer is joining DHT

# This class defines a Peer and will track its state
class Peer:
    def __init__(self, name, address, m_port, p_port, state: PeerState):
        self.name = name
        self.address = address
        self.m_port = m_port
        self.p_port = p_port
        self.state = state

    def set_state(self, state: PeerState):
        self.state = state

# Deprecated: Use setting_up_dht, rebuilding_dht, and tearing_down_dht for waiting
# Enum for manager state flags
class State(IntEnum):
    NORMAL = 0 # Not waiting
    SETUP = 1 # Waiting for dht-complete
    REBUILD = 2 # Waiting for dht-rebuilt
    TEARDOWN = 3 # Waiting for teardown-complete

# Lock for mutex among threads
mutex = Lock()
state_mutex = Lock()
# Dictionary to track Peers and their data
peers = dict()
peer_rebuilder = None
# Dictionary to track used ports
port_used = dict()
# Flags for DHT status
dht_exists = False
setting_up_dht = False
rebuilding_dht = False
tearing_down_dht = False
waiting = State.NORMAL # Deprecated: Use setting_up_dht, rebuilding_dht, and tearing_down_dht for waiting


# Whether the manager is in a waiting state
# command:  The command from a Peer. 
# Only certain commands are allowed during certain waiting states,
# All other commands should return FAILURE.
# Allowed commands in different wait states:
# <wait_state>:     <command>
# setting_up_dht:   dht-complete
# rebuilding_dht:   dht-rebuilt
# tearing_down_dht: teardown-complete
def is_waiting(command: str) -> bool:
    
    # Global variables from manager.data
    global state_mutex
    global setting_up_dht
    global rebuilding_dht
    global tearing_down_dht
    
    with state_mutex:
        # Waiting for dht-complete
        if setting_up_dht:
            if command == 'dht-complete':
                return False
            else:
                logger.info('Error: Cannot complete command because manager is waiting for "dht-complete".')
                return True
            
        # Waiting for dht-rebuilt
        if rebuilding_dht:
            if command == 'dht-rebuilt':
                return False
            else:
                logger.info('Error: Cannot complete command because manager is waiting for "dht-rebuilt".')
                return True
        
        # Waiting for teardown-complete
        if tearing_down_dht:
            if command == 'teardown-complete':
                return False
            else:
                logger.info('Error: Cannot complete command because manager is waiting for "teardown-complete".')
                return True
        
    # Otherwise, manager is not waiting
    return False

# Set the waiting state of the manager
# command:  The command for which to wait
# Command 'None' sets manager to not waiting
def set_waiting(command: str) -> None:

    # Global variables from manager.data
    global state_mutex
    global setting_up_dht
    global rebuilding_dht
    global tearing_down_dht

    if command == 'dht-complete': # Waiting for 'dht-complete'
        with state_mutex:
            setting_up_dht = True
    elif command == 'dht-rebuilt': # Waiting for 'dht-rebuilt'
        with state_mutex:
            rebuilding_dht = True
    elif command == 'teardown-complete': # Waiting for 'teardown-complete'
        with state_mutex:
            tearing_down_dht = True
    elif command == 'None': # No longer waiting
        with state_mutex:
            setting_up_dht = False
            rebuilding_dht = False
            tearing_down_dht = False


# DEPRECATED
# Returns whether the manager is waiting for a signal
# this_command: 
#   optional parameter for commands that set flags
#   (dht-complete, dht-rebuilt, teardown-complete)
def is_waiting_old(this_command = State.NORMAL) -> bool:
    # Global declarations
    global waiting
    global state_mutex

    with state_mutex:
        logger.debug('is_waiting(%s), while waiting = %s', this_command.name, waiting.name)
        if waiting == State.NORMAL:
            # Manager is not waiting
            return False
        elif waiting == this_command:
            # Manager is waiting for this command
            return False
        else:
            # Manager is waiting
            return True


# DEPRECATED
# Set the waiting state of the manager
# command:
#   State of waiting to set the Manager
def set_waiting_old(command: State) -> None:
    # Declare global variables
    global waiting
    global state_mutex

    with state_mutex:
        logger.debug('set_waiting(%s), while waiting = %s', command.name, waiting.name)
        waiting = command  