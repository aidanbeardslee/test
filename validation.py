# Author:  Joseph Thalman
# This program implements input validation methods for the 
#  manager's command handlers.

import socket
from threading import Thread, Lock
import json
from enum import IntEnum
import logging
import traceback
from sys import exit

from . import data as Data
from .data import Peer, PeerState, SUCCESS, FAILURE

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# Contains validation functions for all command function inputs
class Validate():

    # Validate input parameters and manager state for "register" command
    # param:
    #   peer-name, IPv4-address, m-port, p-port
    def register(param) -> bool:

        logging.debug('Validating register params for %s...', param)
        
        # Validate command length
        if not (len(param) == 4):
            logging.info('Error: Invalid number of register parameters: %s', param)
            return False 
        
        # Validate fields
        if not ('peer-name' in param
                and 'IPv4-address' in param
                and 'm-port' in param
                and 'p-port' in param):
            logging.info('Error: Invalid register fields: %s', param)
            return False
        
        # Validate peer-name length
        if (len(param['peer-name']) > 15) or (len(param['peer-name']) < 1):
            logging.info('Error: peer-name length invalid: %s', param)
            return False
        
        with Data.mutex:
            # Check for existing registration
            if param['peer-name'] in Data.peers:
                logger.info('Error: Peer name already in use.')
                logger.debug('Registered Data.peers: %s', Data.peers.keys())
                return False
            
            # Check if manager port is in use
            if param['m-port'] in Data.port_used:
                logger.info('Error: Manager port %s already in use.', param['m-port'])
                logger.debug('Ports in use: %s', Data.port_used.keys())
                return False

            # Check if Peer port is in use
            if param['p-port'] in Data.port_used:
                logger.info('Error: Peer port %s already in use.', param['p-port'])
                logger.debug('Ports in use: %s', Data.port_used.keys())
                return False
        
        return True

    # Validate input parameters and manager state for "setup-dht" command
    # param:
    #   peer-name, n, YYYY
    def setup_dht(param) -> bool:

        # Validate param length
        if not (len(param) == 3):
            logger.info('Error: Invalid number of setup-dht parameters: %s', param)
            return False
        
        # Validate fields
        if not ('peer-name' in param
                and 'n' in param
                and 'YYYY' in param):
            logger.info('Error: Invalid setup-dht fields, expecting "peer-name, n, YYYY": %s', param)
            return False
        
        # Extract parameters
        leader_name = param['peer-name']
        n = int(param['n'])
        year = int(param['YYYY'])

        # n must be >= 3
        if (n < 3):
            logger.info('Error: Invalid setup-dht n value, expects n => 3: %s', param)
            return False
        
        # year must be 1950 <= year <= 2019
        if (year < 1950) or (year > 2019):
            logger.info('Error: Invalid setup-dht YYYY, expects (1950 <= YYYY <= 2019): %s', param)
            return False

        # Aquire access to shared resources
        with Data.mutex:  
            # Leader must be registered
            if not (leader_name in Data.peers):
                logger.info('Error: Peer must be a registered to initiate setup-dht')
                logger.debug('param: %s,\nRegistered Data.peers: %s', param, Data.peers)
                return False

            # Must have >= n Data.peers registerd
            if (len(Data.peers) < n):
                logger.info('Error: %s or more users must be registered.  There are current only %s.', str(n), len(Data.peers))
                logger.debug('n = %s, len(Data.peers) = %s, param = %s', n, len(Data.peers), param)
                return False

            # DHT cannot already exist
            if Data.dht_exists:
                logger.info('Error:  DHT already exists.')
                return False
        
        return True

    # Validate input parameters and manager state for "dht-complete" command
    # param:
    #   peer-name
    def dht_complete(param) -> bool:
        
        # Validate param length
        if not (len(param) == 1):
            logger.info('Invalid number of dht-complete parameters, expects 1: %s', param)
            return False
        
        # Validate fields
        if not ('peer-name' in param):
            logger.info('Invalid dht-complete fields, expects "peer-name": %s', param)
            return False
        
        peer_name = param['peer-name']

        with Data.mutex:
            # Check peer is registered
            if not (peer_name in Data.peers):
                logger.info('Error: Peer %s is not registered', peer_name)
                return False
            
            # Check peer is leader of DHT
            if not (Data.peers[peer_name].state == PeerState.LEADER):
                logger.info('Error: %s must be a leader to perform dht-complete', peer_name)
                return False   
        
        return True

    # Validate input parameters and manager state for "query-dht" command
    # param:
    #   peer-name
    def query_dht(param) -> bool:

        # Validate param length
        if not (len(param) == 1):
            logger.info('Invalid number of query-dht parameters, expects 1: %s', param)
            return False
        
        # Validate fields
        if not ('peer-name' in param):
            logger.info('Invalid query-dht fields, expects "peer-name": %s', param)
            return False
        
        with Data.mutex:
            # Check if DHT exists
            if not Data.dht_exists:
                logger.info('Error:  DHT must be present for query-dht command.')
                return False
            
            # Check that peer is registered
            if not Data.peers[param['peer-name']]:
                logger.info('Error:  Peer %s must be registered to initiate query-dht command.', param['peer-name'])
                return False
            
            # Check that peer is free
            if not (Data.peers[param['peer-name']].state == PeerState.FREE):
                logger.info('Error:  Peer %s cannot initiate query-dht command while in a DHT.', param['peer-name'])
                return False

        return True

    # Validate input parameters and manager state for "leave-dht" command
    # param
    #   peer-name
    def leave_dht(param) -> bool:

        # Validate param length
        if not (len(param) == 1):
            logger.info('Invalid number of leave-dht parameters, expects 1: %s', param)
            return False
        
        # Validate fields
        if not ('peer-name' in param):
            logger.info('Invalid leave-dht fields, expects "peer-name": %s', param)
            return False
        
        with Data.mutex:       
            # Check if DHT exists
            if not Data.dht_exists:
                logger.info('Error: Peer %s cannot leave the DHT because it does not exist.', param['peer-name'])
                return False
            
            # Check that Peer is in DHT ring
            if not (Data.peers[param['peer-name']].state == PeerState.IN_DHT):
                logger.info('Error: Peer %s cannot leave the DHT because it is not in it.', param['peer-name'])
                return False

        return True

    # Validate input parameters and manager state for "join-dht" command
    def join_dht(param) -> bool:

        # Validate param length
        if not (len(param) == 1):
            logger.info('Invalid number of join-dht parameters, expects 1: %s', param)
            return False
        
        # Validate fields
        if not ('peer-name' in param):
            logger.info('Invalid join-dht fields, expects "peer-name": %s', param)
            return False

        with Data.mutex:       
            # Check if DHT exists
            if not Data.dht_exists:
                logger.info('Error: Peer %s cannot join the DHT because it does not exist.', param['peer-name'])
                return False
            
            # Check that Peer is free
            if not (Data.peers[param['peer-name']].state == PeerState.FREE):
                logger.info('Error: Peer %s must be FREE to join the DHT.', param['peer-name'])
                return False
        
        return True

    # Validate input parameters and manager state for "dht-rebuilt" command
    # param:
    #   peer-name, new-leader
    def dht_rebuilt(param) -> bool:

        # Validate number of parameters
        if not (len(param) == 2):
            logger.info('Invalid number of dht-rebuilt parameters, expects 2: %s', param)
            return False
        
        # Validate parameter field(s)
        if not ('peer-name' in param
                and 'new-leader' in param):
            logger.info('Invalid dht-rebuilt fields, expects "peer-name", "new-leader": %s', param)
            return False
        
        with Data.mutex:
            # Ensure initiating peer has been tracked
            if not Data.peer_rebuilder:
                logger.info('Error: No record of peer leaving or joining DHT.')
                return False

            # Validate correct peer
            if not param['peer-name'] == Data.peer_rebuilder.name:
                logger.info('Error: Peer %s did not initiate a join-dht or leave-dht command.', param['peer-name'])
                return False

        return True

    # Validate input parameters and manager state for "deregister" command
    # param
    #   peer-name
    def deregister(param) -> bool:

        # Validate number of parameters
        if not (len(param) == 1):
            logger.debug('Invalid number of parameters for deregister: %s', param)
            return False
        
        # Validate fields
        if not ('peer-name' in param):
            logger.debug('Invalid fields for deregister: %s', param)
            return False
        
        with Data.mutex:
            # Check if peer is registered
            if not Data.peers[param['peer-name']]:
                logger.info('Error:  Peer %s is not registered.', param['peer-name'])
                return False
            
            # Check if peer is free
            if not (Data.peers[param['peer-name']].state == PeerState.FREE):
                logger.info('Error: Peer is not free.')
                logger.info('Cannot deregister peer %s because its state is %s', param['peer-name'], Data.peers[param['peer-name']].state)
                return False  

        return True

    # Validate input parameters and manager state for "teardown-dht" command
    # param:
    #   peer-name
    def teardown_dht(param) -> bool:

        # Validate param length
        if not (len(param) == 1):
            logger.info('Invalid number of teardown-dht parameters, expects 1: %s', param)
            return False
        
        # Validate fields
        if not ('peer-name' in param):
            logger.info('Invalid teardown-dht fields, expects "peer-name": %s', param)
            return False
        
        with Data.mutex:
            # Check that DHT exists
            if not Data.dht_exists:
                logger.info('Error:  Peer %s cannot initiate teardown-dht because DHT does not exist', param['peer-name'])
                return False
            
            # Check that peer is leader of DHT
            if not (Data.peers[param['peer-name']].state == PeerState.LEADER):
                logger.info('Error:  Peer %s cannot initiate teardown-dht because they are not LEADER.', param['peer-name'])
                return False

        return True


    # Validate input parameters and manager state for "teardown-complete" command
    # param:
    #   peer-name
    def teardown_complete(param) -> bool:

        # Validate param length
        if not (len(param) == 1):
            logger.info('Invalid number of teardown-complete parameters, expects 1: %s', param)
            return False
        
        # Validate fields
        if not ('peer-name' in param):
            logger.info('Invalid teardown-complete fields, expects "peer-name": %s', param)
            return False
        
        with Data.mutex:
            # Check that peer is leader of DHT
            if not (Data.peers[param['peer-name']].state == PeerState.LEADER):
                logger.info('Error:  Peer %s cannot initiate teardown-complete because they are not LEADER.', param['peer-name'])
                return False
        
        return True

