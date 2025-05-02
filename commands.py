# Author:  Joseph Thalman
# This program implements the handlers for commands received by 
#  the manager from a peer

import socket
import json
import logging
import traceback
from threading import Thread, Lock
from enum import IntEnum
from sys import exit

from . import data as Data
from .data import Peer, PeerState, SUCCESS, FAILURE
from .validation import Validate


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class Command():
        
    # Register a peer with the manager
    # param:
    #   peer_name, IPv4-address, m_port, p_port
    def register(param) -> dict[str, str]:

        # Validate command
        if not Validate.register(param):
            logger.debug('Invalid register command: %s', param)
            return FAILURE
        
        logger.info('Registering %s', param['peer-name'])
            
        # Create peer registry
        peer = Peer(param['peer-name'], param['IPv4-address'], param['m-port'], param['p-port'], PeerState.FREE)

        # Gain access to shared resources
        with Data.mutex:
            # Store peer
            Data.peers[peer.name] = peer

            # Update used ports
            Data.port_used[peer.m_port] = True
            Data.port_used[peer.p_port] = True

        # Return SUCCESS upon completed registry
        logger.info('Registration complete for %s', peer.name)
        logger.debug('Registered peers: %s', str(Data.peers.keys()))
        return SUCCESS


    # Set up Peers to begin construction of DHT
    # If successful, forces all other commands to return FAILURE until 
    #   dht-complete command is received
    # param:
    #   peer-name n YYYY
    def setup_dht(param) -> dict[str, str]:

        # Validate data
        n = int(param['n']) # number of peers in DHT
        leader_name = param['peer-name'] 
        if not Validate.setup_dht(param):
            logger.debug('Invalid setup-dht command', param)
            return FAILURE

        logger.info('Initiating setup-dht for %s', param['peer-name'])

        response = FAILURE # Default response

        peers_added = 0 # number of peers that will be added
        dht_peers = [] # List of peers in-dht

        with Data.mutex:
            # Set peer as leader
            Data.peers[leader_name].set_state(PeerState.LEADER)

            # Get n - 1 free peers
            for peer in Data.peers.values():

                if (peers_added == n - 1):
                    break # Collected enough free peers

                # Collect Free Peers
                if peer.state == PeerState.FREE:
                    # Add peer to construction list
                    peer_tuple = (peer.name, peer.address, peer.p_port)
                    dht_peers.append(peer_tuple)
                    peer.set_state(PeerState.IN_DHT) # Mark peer as in-dht
                    peers_added += 1

            # If could not find enough free peers        
            if not (peers_added == n - 1):
                for peer in dht_peers:
                    Data.peers[peer[0]].set_state(PeerState.FREE) # Reset state of peer

                # Reset peer marked as leader
                Data.peers[leader_name].set_state(PeerState.FREE)

                logger.info('Not enough free peers for setup-dht')
                return FAILURE

            # Add leader to front of list
            leader = Data.peers[leader_name] # Get Peer object for leader
            leader_tuple = (leader.name, leader.address, leader.p_port)
            dht_peers.insert(0, leader_tuple)

        # Contruct response message
        response = {
            'status': 'SUCCESS'
        }
        response['peers'] = dht_peers # Add peers to response

        # Put manager into waiting state until "dht-complete" command is received
        Data.set_waiting('dht-complete')
        logger.info('setup-dht successful for %s, awaiting dht-complete...', leader_name)

        return response


    # Marks DHT completion status and allows further commands from Peers
    # param:
    #   peer-name
    def dht_complete(param) -> dict[str, str]:

        # Validate data
        if not Validate.dht_complete(param):
            logger.debug('Invalid dht-complete command: %s', param)
            return FAILURE
        
        logger.info('Initiating dht-complete from %s', param['peer-name'])

        # Update flags
        with Data.mutex:
            logger.debug('Data.dht_exists: %s', str(Data.dht_exists))
            Data.dht_exists = True
            logger.debug('Data.dht_exists: %s', str(Data.dht_exists))

        # Take manager out of waiting state
        Data.set_waiting('None') 
        logger.info('dht-complete received.  Resuming normal operation.')

        return SUCCESS


    # Allows a FREE Peer to query the existing DHT.
    # param:
    #   peer-name
    def query_dht(param) -> dict[str, str]:

        # Validate parameters
        if not Validate.query_dht(param):
            logger.info('Error:  Invalid query-dht parameters:  %s', param)
            return FAILURE

        peer_tuple = None # tuple to return if succesful
        with Data.mutex:
            # Find random peer in DHT ring
            for peer in Data.peers.values():
                if (peer.state == PeerState.IN_DHT) or (peer.state == PeerState.LEADER):
                    peer_tuple = (peer.name, peer.address, peer.p_port)
                    break
            
            # If peer not found, return FAILURE
            if peer_tuple == None:
                logger.info('Error: Peer in DHT ring not found.')
                return FAILURE

        # Create message
        response = SUCCESS
        response['parameters'] = peer_tuple

        return response


    # Allows a Peer to leave the existing DHT
    # Puts manager into waiting state until "dht-rebuilt" command
    #  is received.
    # param:
    #   peer-name
    def leave_dht(param) -> dict[str, str]:

        # Validate parameters
        if not Validate.leave_dht(param):
            logger.info("Error:  Invalid parameters for leave-dht:  %s", param)
            return FAILURE
        
        with Data.mutex:
            logger.info('Peer %s leaving DHT.', param['peer-name'])

            # Set peer to LEAVING
            Data.peers[param['peer-name']].set_state(PeerState.LEAVING)

            # Store peer that left
            Data.peer_rebuilder = Data.peers[param['peer-name']]

            logger.info('Peer %s left the DHT.', param['peer-name'])

        # Put manager into waiting state until "dht-rebuilt" command is received
        Data.set_waiting('dht-rebuilt')
        logger.info('Waiting for dht-rebuilt...')

        return SUCCESS


    # Allows a Peer to join an existing DHT
    # Requires the DHT to be rebuilt
    # param:
    #   peer-name
    def join_dht(param) -> dict[str, str]:

        # Validate parameters
        if not Validate.join_dht(param):
            logger.info("Error:  Invalid parameters for join-dht:  %s", param)
            return FAILURE
        
        with Data.mutex:
            logger.info('Peer %s joining DHT.',param['peer-name'])

            # Set peer to JOINING
            Data.peers[param['peer-name']].set_state(PeerState.JOINING)

            # Store peer that joined
            Data.peer_rebuilder = Data.peers[param['peer-name']]

            logger.info('Peer %s joined the DHT.', param['peer-name'])

        # Put manager into waiting state until "dht-rebuilt" command is received
        Data.set_waiting('dht-rebuilt')

        logger.info('Waiting for dht-rebuilt...')
        return SUCCESS


    # The command to signal DHT has been rebuilt.
    # Allows manager to resume normal operation.
    # param:
    #   peer-name new-leader
    def dht_rebuilt(param) -> dict[str, str]:
        
        # Validate parameters
        if not Validate.dht_rebuilt(param):
            logger.info("Error:  Invalid parameters for dht-rebuilt:  %s", param)
            return FAILURE
        
        with Data.mutex:
            # Save desired state of initiating peer
            if Data.peer_rebuilder.state == PeerState.LEAVING:
                Data.peers[param['peer-name']].sate = PeerState.FREE
                logger.info('dht-rebuilt received, %s has left the DHT.', param['peer-name'])
            elif Data.peer_rebuilder.state == PeerState.JOINING:
                Data.peers[param['peer-name']].sate = PeerState.IN_DHT
                logger.info('dht-rebuilt received, %s has joined the DHT.', param['peer-name'])
            else:
                return FAILURE
            
            # Reset peer_rebuilder
            Data.peer_rebuilder = None
            
            # Reset state of old leader if needed
            if not (Data.peers[param['new-leader']].state == PeerState.LEADER):
                for peer in Data.peers.values():
                    if peer.state == PeerState.LEADER:
                        peer.state = PeerState.IN_DHT
                        Data.peers[param['new-leader']].state = PeerState.LEADER
                        break
        
        logger.info('%s is now the leader of the DHT.', param['new-leader'])

        # Take manager out of waiting state
        Data.set_waiting('None')

        logger.info('Resuming normal operation.')
        return SUCCESS


    # Remove peer from registry
    # param:
    #   peer-name
    def deregister(param) -> dict[str, str]:

        # Validate parameters
        if not Validate.deregister(param):
            logger.info('Error: Invalid parameter(s) for command deregister: %s', param)
            return FAILURE
        
        # Display command initiation
        logger.info('Initiating deregister for %s', param['peer-name'])

        with Data.mutex:
            # Remove and retrieve peer
            peer = Data.peers.pop(param['peer-name'])

            # Remove peers ports
            Data.port_used.pop(peer.m_port)
            Data.port_used.pop(peer.p_port)

        logger.info('Deregister successful for %s', param['peer-name'])
        logger.debug('Registered peers: %s', str(Data.peers.keys()))
        return SUCCESS

    # Starts the teardown process of the DHT by peers.
    # Puts manager into waiting state until "teardown-complete"
    #  command is received.
    # param:
    #   peer-name
    def teardown_dht(param) -> dict[str, str]:
        
        # Validate parameters
        if not Validate.teardown_dht(param):
            logger.info("Error:  Invalid parameters for teardown-dht:  %s", param)
            return FAILURE
        
        # Put manager into waiting state until 'teardown-complete' command is received
        Data.set_waiting('teardown-complete')

        logger.info('DHT teardown initiated, waiting for teardown-complete...')
        return SUCCESS

    # Signal that the teardown of the DHT by peers is complete.
    # Allows the manager to resume normal operation.
    # param:
    #   peer-name
    def teardown_complete(param) -> dict[str, str]:
        
        # Validate parameters
        if not Validate.teardown_dht(param):
            logger.info("Error:  Invalid parameters for teardown-complete:  %s", param)
            return FAILURE
        
        with Data.mutex:
            # Change all peers' state to FREE
            for peer in Data.peers.values():
                peer.state = PeerState.FREE

            # Track deletion of DHT
            Data.dht_exists = False
        
        # Take manager out of waiting state
        Data.set_waiting('None')

        logger.info('teardown-complete received, resuming normal operations.')
        return SUCCESS
