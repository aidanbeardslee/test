# Author: Brandon Ramirez
# Main file to run the peer commands
# How to use: python3 peer_main.py <manager_ip> <manager_port>, where manager_port is the listening port of the manager
# peer name: for each window use a dif one
# port for manager communication: m_port, receiving port from manager (unique for each)
# port for peer communication: p_port, p2p communication (unique for each)

import socket
import json
import sys 
import threading
from peer.peer_dht import PeerDHT

peer_dht = PeerDHT()

# Continuously process peer-peer messages
def receive_messages(sock):
    while True:
        try:
            data, addr = sock.recvfrom(4096)
            message = json.loads(data.decode())
            command = message.get("command")
            
            print(f"Received {command} from {addr}")
            peer_dht.handle_command(command, message)
            
        except Exception as e:
            print(f"Error receiving message: {str(e)}")

# Set initial data and allow user-init peer-manager messages
def main():
    if len(sys.argv) != 3:
        print("Usage: python3 peer_main.py <manager_ip> <manager_port>")
        return

    # Define manager address      
    manager_ip = sys.argv[1]
    manager_port = int(sys.argv[2])
    
    # Gather peer info
    peer_name = input("Enter peer name: ")
    peer_ip = input("Enter peer IPv4 addres: ")
    m_port = int(input("Enter port for manager communication: "))
    p_port = int(input("Enter port for peer communication: "))
    
    peer_dht.set_peer_info(peer_name, manager_ip, manager_port)

    # Create peer-peer and peer-manager sockets 
    manager_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    peer_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    manager_sock.bind((peer_ip, m_port))
    peer_sock.bind((peer_ip, p_port))
    
    # Save socket info
    peer_dht.set_socks(manager_sock, peer_sock)
    peer_dht.ring_manager.set_socks(manager_sock, peer_sock)
    
    # Start listening for peer-peer comms
    listener_thread = threading.Thread(target=receive_messages, args=(peer_sock,), daemon=True)
    listener_thread.start()
    
    # Listen for user-init peer-manager messages
    while True:
        command = input("> ")
        parts = command.split()
        
        # Handle different commands
        if parts[0] == "register": # Handle register

            # Validate input
            if len(parts) != 5:
                print("Usage: register <peer-name> <IPv4-address> <m-port> <p-port>")
                continue

            # Construct message for manager     
            message = {
                "command": "register",
                "parameters": {
                    "peer-name": parts[1],
                    "IPv4-address": parts[2],
                    "m-port": int(parts[3]),
                    "p-port": int(parts[4])
                }
            }
            
            print("Sending register to manager...")
            manager_sock.sendto(json.dumps(message).encode(), (manager_ip, manager_port))
            
            # Receive and display manager response
            data, _ = manager_sock.recvfrom(4096)
            response = json.loads(data.decode())
            print(f"Manager response: {response['status']}")
            
        elif parts[0] == "setup-dht": # Handle setup-dht
            # parts: setup-dht, peer-name, n, YYYY

            # Validate input
            if len(parts) != 4:
                print("Usage: setup-dht <peer-name> <n> <YYYY>")
                continue

            # Construct message for manager     
            message = {
                "command": "setup-dht",
                "parameters": {
                    "peer-name": parts[1],
                    "n": int(parts[2]),
                    "YYYY": parts[3]
                }
            }
            
            # Send message to manager
            print("Sending setup-dht to manager...")
            manager_sock.sendto(json.dumps(message).encode(), (manager_ip, manager_port))
            
            # Receive message from manager
            data, _ = manager_sock.recvfrom(4096)
            response = json.loads(data.decode())
            
            if response["status"] == "SUCCESS":
                print(f"Manager response: {response['status']}")
                print("Setting up DHT as leader...")
                peer_dht.set_csv_file(parts[3]) # Define csv for given year

                # Begin peer setup-dht process
                # This will send dht-complete to manager upon success
                peer_dht.handle_setup_dht_success(response["peers"]) 
            else:
                print(f"Manager response: {response['status']}")

        elif parts[0] == "join-dht":
            if len(parts) != 2:
                print("Usage: join-dht <peer-name>")
                continue
                
            message = {
                "command": "join-dht",
                "parameters": {
                    "peer-name": parts[1]
                }
            }
            
            manager_sock.sendto(json.dumps(message).encode(), (manager_ip, manager_port))
            
            data, _ = manager_sock.recvfrom(4096)
            response = json.loads(data.decode())
            
            if response["status"] == "SUCCESS":
                print("Join-DHT successful, waiting for DHT to be rebuilt")
            else:
                print(f"Manager response: {response['status']}")

                
        elif parts[0] == "exit": # Handle exit
            break # Stops reading commands
            
        else:
            print(f"Unknown command: {parts[0]}")
    
    print("Exiting...")

if __name__ == "__main__":
    main()