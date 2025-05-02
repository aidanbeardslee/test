# Author:  Joseph Thalman
# This program defines a manager for a DHT system.

import socket
import json
import logging
import traceback
from sys import exit
from threading import Thread, Lock
from enum import IntEnum

import manager.data as Data
from manager.data import SUCCESS, FAILURE, BUFFER_SIZE
from manager.commands import Command

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


# Handle a new Peer connection
def handle_peer(manager_socket, peer_data, peer_address) -> None:
    
    # DEBUG checking dht_exists status
    with Data.mutex:
        logger.debug('dht_exists: %s', str(Data.dht_exists))

    # Unpack JSON data
    message = json.loads(peer_data.decode())

    # Seperate message into command and parameters
    command = message['command']
    param = message['parameters']

    logger.info('Data received from peer w/ address %s: %s', peer_address, peer_data.decode())
    logger.info('Peer w/ address %s initiating command: %s', peer_address, command)

    response = FAILURE

    # Check if manager is waiting
    if Data.is_waiting(command):
        # Do nothing and return failure
        respnose = FAILURE
    else:
        # Perform the requested operation
        try:
            if command == 'register':
                response = Command.register(param)
            elif command == 'setup-dht':
                response = Command.setup_dht(param)
            elif command == 'dht-complete':
                response = Command.dht_complete(param)
            elif command == 'query-dht':
                response = Command.query_dht(param)
            elif command == 'leave-dht':
                response = Command.leave_dht(param)
            elif command == 'join-dht':
                response = Command.join_dht(param)
            elif command == 'dht-rebuilt':
                response = Command.dht_rebuilt(param)
            elif command == 'deregister':
                response = Command.deregister(param)
            elif command == 'teardown-dht':
                response = Command.teardown_dht(param)
            elif command == 'teardown-complete':
                response = Command.teardown_complete(param)
            else:
                response = FAILURE
        except Exception as ex:
            logger.debug('Exception traced to handlePeer(), sending FAILURE: %s', ex)
            logger.debug('%s', traceback.format_exc())
            response = FAILURE

    # Return the result status of the operation
    logger.info('Manager sending to %s: %s', peer_address, response)
    manager_socket.sendto(json.dumps(response).encode(), peer_address)

    # DEBUG checking dht_exists status
    with Data.mutex:
        logger.debug('dht_exists: %s', str(Data.dht_exists))

    return 


# Listen for new connections
# Currently has no means of returning/terminating
def listen() -> None:

    # Retrieve port number for the manager to use
    # port number for Group 19 must be in range [10500, 10999]
    portIsValid = False
    while not portIsValid:
        manager_port = int(input("Enter manager port number: "))

        # Port number validation
        if (manager_port < 10500 or manager_port > 10999):
            # Invalid port, print error and try again
            print('Error:  Group 19 port numbers must be in range [10500, 10999]')
        else:
            # Port is valid, proceed
            portIsValid = True
    Data.port_used[manager_port] = True # Mark manager port as used

    manager_ip = input("Enter manager IPv4 address: ")

    # Create UDP socket for manager using IPv4
    manager_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Bind the manager socket and IP address (localhost)' 
    manager_socket.bind((manager_ip, manager_port))

    logger.info("Listening started...")
    threads = [] # List to track threads
    try:

        logger.info('Manager now receiving at (%s, %s)', manager_ip, manager_port)

        # Endlessly listen for new Peers
        while True:

            # Accept new Peer payload
            peer_data, peer_address = manager_socket.recvfrom(BUFFER_SIZE)

            # If data received, create thread to handle command
            if peer_data:

                logger.info('Data received from %s', peer_address)

                # Create new thread to handle Peer connection
                peer = Thread(target=handle_peer, args=(manager_socket, peer_data, peer_address))
                threads.append(peer)

                # Start peer's handler
                with Data.mutex:
                    logger.debug('dht_exists: %s', str(Data.dht_exists))
                logger.debug('Thread started for Peer with address %s', peer_address)
                peer.start()
                with Data.mutex:
                    logger.debug('dht_exists: %s', str(Data.dht_exists))
                

                

            # After peer process started, loop begins again

    except Exception as ex:
        logger.debug('Error in listen(): %s', ex)
        logger.debug('%s', traceback.format_exc())
    
    finally:
        logger.info('Manager no longer listening.')

        # Kill all threads 
        for thread in threads:
            thread.join()

        logger.debug('All Peer threads killed.')

        # Close manager socket
        manager_socket.close()

        logger.debug('Manager socket closed.')


# Main process for waiting for messages
if __name__ == '__main__':

    # Create child thread for listening for peer connections
    listening_thread = Thread(target=listen)
    try:
        # Start listening
        listening_thread.start()

    except Exception as ex:
        logger.debug('Error from main: %s', ex)
        logger.debug('%s', traceback.format_exc())  

    finally:
        # Wait for listening to end to end the program
        listening_thread.join()
                
        logger.debug("All threads killed.")
        logger.info("Exiting manager.")
