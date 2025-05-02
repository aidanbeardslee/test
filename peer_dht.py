# Author: Brandon Ramirez, Carlos Ramirez, Joseph Thalman

import logging
import csv
import gzip
import sympy
import traceback
import json
import random
import socket
from .ring_manager import RingManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("PeerDHT")

class PeerDHT:
    def __init__(self):
        self.ring_manager = RingManager()
        self.peer_name = None
        self.manager_addr = None
        self.manager_port = None
        self.local_hash_table = {} 
        self.csv_file = None
        self.m_sock = None
        self.p_sock = None


    def set_peer_info(self, peer_name, manager_addr, manager_port):
        self.peer_name = peer_name
        self.manager_addr = manager_addr
        self.manager_port = manager_port


    # Set local path to csv file
    # csv files are compressed into csv.gz files
    def set_csv_file(self, year):
        self.csv_file = f"data/StormEvents_details-ftp_v1.0_d{year}.csv.gz"


    # Set sockets used for peer-peer and peer-manager messages
    def set_socks(self, m_sock, p_sock):
        self.m_sock = m_sock
        self.p_sock = p_sock


    # Initial setup of DHT after setup-dht command is successful    
    def handle_setup_dht_success(self, peers):
        # PART 1a
        logger.info(f"Handling successful setup-dht with {len(peers)} peers")
        
        # Set current peer as leader
        self.ring_manager.set_as_leader(peers)
        
        # Set Peer IDs in the ring
        success = self.ring_manager.setup_ring()

        if success and self.ring_manager.is_leader and self.csv_file:
            self.process_csv_file() # Read data from file
        
        return success
    

    # Find next prime greater than (2 * n)
    def find_next_prime(self, n):
        return sympy.nextprime(2 * n)
    

    # Compute position and peer_id responsible for storing record
    def compute_hash(self, event_id, hash_table_size, ring_size):
        pos = event_id % hash_table_size
        peer_id = pos % ring_size
        return pos, peer_id
    

    # Store record in local hash table
    def store_record(self, event_id, record, pos):
        self.local_hash_table[pos] = record
        logger.info(f"Stored record with event_id {event_id} at position {pos}")


    # Read csv file into records and store among peers in the ring
    def process_csv_file(self):

        # Validate Peer leader state
        if not self.ring_manager.is_leader:
            logger.warning("Only the leader should process the CSV file")
            return False
        
        try:
            logger.info(f"Leader processing CSV file: {self.csv_file}")
            
            # Read data from file into records
            records = []
            with gzip.open(self.csv_file, mode='rt') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    records.append(row)
            
            num_records = len(records)
            logger.info(f"Found {num_records} records in the CSV file")
            
            hash_table_size = self.find_next_prime(num_records)
            logger.info(f"Hash table size set to {hash_table_size}")
            
            # Calculate record storage positions
            for record in records:
                event_id = int(record["EVENT_ID"])
                
                pos, target_peer_id = self.compute_hash(
                    event_id, 
                    hash_table_size, 
                    self.ring_manager.ring_size
                )
                
                # Store the record
                if target_peer_id == self.ring_manager.my_id:
                    self.store_record(event_id, record, pos)
                else:
                    self.send_store_command(event_id, record, pos, target_peer_id)

                # Track record storage
                self.ring_manager.update_record_count(node_id=target_peer_id)
            
            logger.info(f"Completed processing {num_records} records")
            logger.info("Leader signaling DHT completion")

            # Print DHT and send dht-complete to manager
            self.complete_dht_setup()
            
            return True
        
        except Exception as e:
            logger.error(f"Error processing CSV file: {str(e)}")
            logger.error('%s', traceback.format_exc())
            return False


    # Forward store command to next peer in the ring    
    def send_store_command(self, event_id, record, pos, target_peer_id):
        message = {
            "command": "store",
            "parameters": {
                "event_id": event_id,
                "record": record,
                "pos": pos,
                "target_peer_id": target_peer_id
            }
        }
        
        self.ring_manager.forward_message(message)
    

    # Process peer-peer commands
    def handle_command(self, command, data):
        if command == "set-id":
            # Part 1b
            logger.info("Received set-id command")
            return self.ring_manager.handle_set_id(data)
        
        elif command == "store":
            # Part 2
            logger.info("Received store command")
            
            params = data.get("parameters", {})
            event_id = params.get("event_id")
            record = params.get("record")
            pos = params.get("pos")
            target_peer_id = params.get("target_peer_id")
            
            if target_peer_id == self.ring_manager.my_id:
                self.store_record(event_id, record, pos)
                return True
            else:
                self.ring_manager.forward_message(data)
        
        elif command == "teardown":
            # 1.2.5
            logger.info("Received teardown command")
            result = self.ring_manager.handle_teardown(data)
            
            if result and self.ring_manager.is_leader:
                self.local_hash_table = {}
                self.teardown_complete()
            else:
                self.local_hash_table = {}
                
            return result
        elif command == "find-event":
            self.handle_find_event(data)
        elif command == "find-event-response":
            self.handle_find_event_response(data)
        elif command == "reset-id":
            self.handle_reset_id(data)
        elif command == "rebuild-dht":
            self.handle_rebuild_dht(data)
        return None

    
    def handle_find_event(self, message):
        params = message["parameters"]
        event_id = int(params["event_id"])
        origin = tuple(params["origin"])  # The querying peer's 3-tuple (peer-name, IP, port)
        id_seq = params.get("id_seq", [])  
        hash_table_size = self.find_next_prime(len(self.local_hash_table) * 2 or 10)
        pos, expected_peer_id = self.compute_hash(event_id, hash_table_size, self.ring_manager.ring_size)
        id_seq.append(self.ring_manager.my_id)  
        if self.ring_manager.my_id == expected_peer_id:
            record = self.local_hash_table.get(pos)
            if record and int(record["EVENT_ID"]) == event_id:
                response = {
                    "command": "find-event-response",
                    "parameters": {
                        "status": "SUCCESS",
                        "record": record,
                        "id_seq": id_seq
                    }
                }
            else:
                response = {
                    "command": "find-event-response",
                    "parameters": {
                        "status": "FAILURE",
                        "event_id": event_id,
                        "id_seq": id_seq
                    }
                }
            self.send_udp_message(response, origin)  
        else:
            other_ids = [i for i in range(self.ring_manager.ring_size) if i != self.ring_manager.my_id]
            next_id = random.choice(other_ids) 
            next_peer = self.ring_manager.get_peer_by_id(next_id)
            if not next_peer:
                logger.error("Next peer not found in ring!")
                return
            forward_message = {
                "command": "find-event",
                "parameters": {
                    "event_id": event_id,
                    "origin": origin,
                    "id_seq": id_seq
                }
            }
            self.send_udp_message(forward_message, (next_peer["ip"], next_peer["port"]))
    
    def handle_find_event_response(self, message):
        params = message["parameters"]
        status = params["status"]
        if status == "SUCCESS":
            record = params["record"]
            print("Storm Event Found:")
            for key, value in record.items():
                print(f"{key}: {value}")
            print(f"Visited nodes (id-seq): {params['id_seq']}")
        else:
            print(f"Storm event {params['event_id']} not found in the DHT.")
            print(f"Visited nodes (id-seq): {params['id_seq']}")
    
    def handle_reset_id(self, message):
        params = message["parameters"]
        new_id = params["new_id"]
        ring_size = params["ring_size"]
        peers = params["peers"]
        self.ring_manager.my_id = new_id
        self.ring_manager.ring_size = ring_size
        self.ring_manager.peers = peers
        logger.info(f"Reset my ID to {new_id}, ring size now {ring_size}")
        next_id = new_id + 1
        if next_id < ring_size:
            next_peer = peers[next_id]
            reset_message = {
                "command": "reset-id",
                "parameters": {
                    "new_id": next_id,
                    "ring_size": ring_size,
                    "peers": peers
                }
            }
            self.send_udp_message(reset_message, (next_peer["ip"], next_peer["port"]))
    
    
    def handle_rebuild_dht(self, message):
        params = message["parameters"]
        year = params.get("year")
        logger.info("New leader received rebuild-dht. Rebuilding DHT...")
        self.set_csv_file(year)     
        self.ring_manager.set_as_leader(self.ring_manager.peers) 
        # FIX: Changed self.setup_ring() to self.ring_manager.setup_ring()
        if self.ring_manager.setup_ring() and self.csv_file:
            self.process_csv_file()
        leaver_name = params["leaver"]
        self.signal_dht_rebuilt(new_leader_name=self.peer_name)

    
    def initiate_leave_dht(self):
        logger.info(f"{self.peer_name} is initiating leave-dht process")
        self.initiate_teardown()  # Initiate the teardown process
        reset_message = {
            "command": "reset-id",
            "parameters": {
                "new_id": 0,
                "ring_size": self.ring_manager.ring_size - 1,
                "peers": [p for p in self.ring_manager.peers if p["name"] != self.peer_name]
            }
        }
        right_neighbor = self.ring_manager.get_right_neighbor()
        if right_neighbor:
            # Send the reset-id message to the right neighbor
            self.send_udp_message(reset_message, (right_neighbor["ip"], right_neighbor["port"]))
            logger.info(f"Sent reset-id to right neighbor: {right_neighbor['name']}")
        else:
            logger.error("Could not find right neighbor to send reset-id")
        new_leader = right_neighbor
        # Prepare the rebuild-dht message for the new leader
        rebuild_message = {
            "command": "rebuild-dht",
            "parameters": {
                "leaver": self.peer_name,
                "year": self.csv_file.split('d')[-1][:4]
            }
        }
        self.send_udp_message(rebuild_message, (new_leader["ip"], new_leader["port"]))

    
    def send_udp_message(self, message, addr):
        try:
            self.p_sock.sendto(json.dumps(message).encode(), addr)
        except Exception as e:
            logger.error(f"Error sending UDP message to {addr}: {e}")


    # Print DHT config and send dht-complete to manager
    def complete_dht_setup(self):

        # Validate Peer Leader state
        if not self.ring_manager.is_leader:
            logger.warning("Only the leader can complete DHT setup")
            return False
        
        # Print DHT config
        self.ring_manager.print_dht_configuration()
        
        # Send dht-complete command to manager
        success = self.ring_manager.signal_dht_complete(
            self.peer_name, 
            self.manager_addr, 
            self.manager_port,
        )
        
        return success
    

    def update_record_count(self, node_id=None, increment=1):
        self.ring_manager.update_record_count(node_id, increment)


    def initiate_teardown(self):
        # 1.2.5
        if not self.ring_manager.is_leader:
            logger.warning("Only the leader can initiate teardown")
            return False
        
        success = self.ring_manager.initiate_teardown()
        
        self.local_hash_table = {}
        
        return success


    def teardown_complete(self):
        # 1.2.5
        success = self.ring_manager.signal_teardown_complete(
            self.peer_name,
            self.manager_addr,
            self.manager_port
        )
        
        return success
    

    def handle_join_dht(self, new_peer):
        # 1.2.4
        if not self.ring_manager.is_leader:
            logger.warning("Only the leader can handle join-dht")
            return False
        
        logger.info("Leader initiating teardown for join-dht")
        self.initiate_teardown()
        
        self.local_hash_table = {}
        
        # Add the new peer to the list
        peers = self.ring_manager.peers.copy()
        peers.append(new_peer)
        
        logger.info("Rebuilding DHT with new peer")
        self.ring_manager.set_as_leader(peers)
        success = self.ring_manager.setup_ring()
        
        if success and self.csv_file:
            self.process_csv_file()
        
        success = self.signal_dht_rebuilt(new_peer[0])
        
        return success


    def signal_dht_rebuilt(self, new_leader_name=None):
        # 1.2.4
        if not self.ring_manager.is_leader:
            logger.warning("Only the leader can signal DHT rebuilt")
            return False
        
        if new_leader_name is None:
            new_leader_name = self.peer_name
        
        try:
            message = {
                "command": "dht-rebuilt",
                "parameters": {
                    # FIX: Changed "peer_name" to "peer-name"
                    "peer-name": self.peer_name,
                    "new-leader": new_leader_name
                }
            }
            
            # FIX: Use self.m_sock instead of creating a new socket
            self.m_sock.sendto(json.dumps(message).encode(), (self.manager_addr, self.manager_port))
            logger.info(f"Sent dht-rebuilt to manager at {self.manager_addr}:{self.manager_port}")
            
            # Receive and process response
            data, _ = self.m_sock.recvfrom(4096)
            response = json.loads(data.decode())
            logger.info(f"Manager response to dht-rebuilt: {response['status']}")
            
            return response["status"] == "SUCCESS"
        except Exception as e:
            logger.error(f"Failed to send dht-rebuilt: {str(e)}")
            return False
