# Author: Brandon Ramirez, Joseph Thalman

import socket
import json
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("RingManager")

class RingManager:
    def __init__(self):
        self.my_id = None
        self.ring_size = None
        self.peers = [] 
        self.right_neighbor = None
        self.is_leader = False
        self.node_record_counts = {}
        self.m_sock = None
        self.p_sock = None


    # Set sockets used for peer-peer and peer-manager messages
    def set_socks(self, m_sock, p_sock):
        self.m_sock = m_sock
        self.p_sock = p_sock


    # Set this peer as the leader in the DHT
    def set_as_leader(self, peers):
        # PART 1a
        self.my_id = 0
        self.ring_size = len(peers)
        self.peers = peers
        self.is_leader = True
        
        self.right_neighbor = peers[1] if len(peers) > 1 else peers[0]
        logger.info(f"Set as leader (ID 0) in ring of size {self.ring_size}")
        logger.info(f"Right neighbor: {self.right_neighbor}")
        
        # Initializing record counts
        for i in range(self.ring_size):
            self.node_record_counts[i] = 0
            
        return True
    

    # Set ID for each Peer in the ring
    def setup_ring(self):
        # PART 1a
        logger.info("Setting up the ring as leader...")
        
        # Set ID of each Peer
        for i in range(1, self.ring_size):
            peer_name, ip_addr, p_port = self.peers[i]
            
            # Creating the set-id message
            message = {
                "command": "set-id",
                "parameters": {
                    "id": i,
                    "ring_size": self.ring_size,
                    "peers": self.peers
                }
            }
            
            # Sending the set-id command to the peer
            try:
                self.p_sock.sendto(json.dumps(message).encode(), (ip_addr, p_port))
                logger.info(f"Sent set-id with ID {i} to peer {peer_name} at {ip_addr}:{p_port}")
                
            except Exception as e:
                logger.error(f"Failed to send set-id to peer {peer_name}: {str(e)}")
                return False

        
        logger.info("Ring setup completed")
        return True
    

    # Handle set-id command between Peers
    def handle_set_id(self, data):
        # Part 1b
        try:
            # Extract DHT construction data
            self.my_id = data["parameters"]["id"]
            self.ring_size = data["parameters"]["ring_size"]
            self.peers = data["parameters"]["peers"]
            
            # Determining right neighbor 
            next_id = (self.my_id + 1) % self.ring_size
            self.right_neighbor = self.peers[next_id]
            
            logger.info(f"Set ID to {self.my_id} in ring of size {self.ring_size}")
            logger.info(f"Right neighbor: {self.right_neighbor}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to handle set-id: {str(e)}")
            return False
    

    # Forward message to next Peer in the ring
    def forward_message(self, message):

        # Validate existence of next neighbor
        if not self.right_neighbor:
            logger.error("Cannot forward message: right neighbor not set")
            return False
        
        try:
            # Send message to next neighbor
            _, ip_addr, p_port = self.right_neighbor
            self.p_sock.sendto(json.dumps(message).encode(), (ip_addr, p_port))

            logger.info(f"Forwarded message to right neighbor at {ip_addr}:{p_port}")

            return True
        except Exception as e:
            logger.error(f"Failed to forward message: {str(e)}")
            return False
    

    # Update the tracked number of stored records in the local hash table
    def update_record_count(self, node_id=None, increment=1):
        if node_id is None:
            node_id = self.my_id
        
        if node_id not in self.node_record_counts:
            self.node_record_counts[node_id] = 0
        
        self.node_record_counts[node_id] += increment
    

    # Display relevant DHT information after successful construction
    def print_dht_configuration(self):
        # Part 3 - Print config

        # Validate leader state
        if not self.is_leader:
            logger.warning("Only the leader should print the DHT configuration")
            return
        
        # Log DHT info
        logger.info("DHT Configuration:")
        for node_id in sorted(self.node_record_counts.keys()):
            logger.info(f"Node {node_id}: {self.node_record_counts[node_id]} records")
        
        # Display DHT info 
        print("\nDHT Configuration:")
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("-" * 40)

        # Display records stored at each node
        for node_id in sorted(self.node_record_counts.keys()):
            print(f"Node {node_id}: {self.node_record_counts[node_id]} records")
        print("-" * 40)
        print(f"Total records: {sum(self.node_record_counts.values())}")
    

    # Send dht-complete command to manager
    def signal_dht_complete(self, peer_name, manager_addr, manager_port):
        # Part 3 - Print config
        # Only the leader should signal completion
        if not self.is_leader:
            logger.warning("Only the leader should signal DHT completion")
            return False
        
        try:
            message = {
                "command": "dht-complete",
                "parameters": {
                    "peer-name": peer_name
                }
            }
            
            # Send command to manager
            print("Sending dht-complete to manager...")
            self.m_sock.sendto(json.dumps(message).encode(), (manager_addr, manager_port))
            logger.info(f"Sent dht-complete to manager at {manager_addr}:{manager_port}")

            # Dislay manager response
            data, _ = self.m_sock.recvfrom(4096)
            response = json.loads(data.decode())
            print(f"Manager response: {response['status']}")
            
            return True
        except Exception as e:
            logger.error(f"Failed to send dht-complete: {str(e)}")
            return False
        
    def initiate_teardown(self):
        # 1.2.5
        if not self.is_leader:
            logger.warning("Only the leader can initiate teardown")
            return False
        
        # Teardown message
        message = {
            "command": "teardown",
            "parameters": {
                "originator": self.my_id
            }
        }
        
        # Send to right neighbor
        logger.info("Leader initiating DHT teardown")
        success = self.forward_message(message)
        
        return success

    def handle_teardown(self, data):
        # 1.2.5
        # Delete local hash table
        logger.info("Received teardown command, deleting local hash table")
        
        params = data.get("parameters", {})
        originator = params.get("originator")
        
        if originator == self.my_id:
            logger.info("Teardown propagated around the ring")
            return True
        else:
            self.forward_message(data)
        
        return True

    def signal_teardown_complete(self, peer_name, manager_addr, manager_port):
        # 1.2.5
        if not self.is_leader:
            logger.warning("Only the leader should signal teardown completion")
            return False
        
        try:
            message = {
                "command": "teardown-complete",
                "parameters": {
                    # FIX: Changed "peer_name" to "peer-name"
                    "peer-name": peer_name
                }
            }
            
            # FIX: Use self.m_sock instead of creating a new socket
            self.m_sock.sendto(json.dumps(message).encode(), (manager_addr, manager_port))
            logger.info(f"Sent teardown-complete to manager at {manager_addr}:{manager_port}")
            
            # Receive and process response
            data, _ = self.m_sock.recvfrom(4096)
            response = json.loads(data.decode())
            logger.info(f"Manager response to teardown-complete: {response['status']}")
            
            return response["status"] == "SUCCESS"
        except Exception as e:
            logger.error(f"Failed to send teardown-complete: {str(e)}")
            return False
    
    def get_peer_by_id(self, peer_id):
        if peer_id is None or peer_id < 0 or peer_id >= self.ring_size:
            logger.warning(f"Invalid peer ID: {peer_id}")
            return None
        
        if peer_id < len(self.peers):
            peer = self.peers[peer_id]
            return {
                "id": peer_id,
                "name": peer[0],
                "ip": peer[1],
                "port": peer[2]
            }
        else:
            logger.warning(f"Peer ID {peer_id} out of range")
            return None
    
    def get_right_neighbor(self):
        if self.my_id is None:
            logger.warning("My ID is not set")
            return None
        
        right_id = (self.my_id + 1) % self.ring_size
        if 0 <= right_id < len(self.peers):
            peer = self.peers[right_id]
            return {
                "id": right_id,
                "name": peer[0],
                "ip": peer[1],
                "port": peer[2]
            }
        else:
            logger.warning(f"Right neighbor ID {right_id} out of range")
            return None
