import socket
import threading
import json
import time
import sys
import os
from blockchain import Blockchain

class BlockchainPeer:
    def __init__(self, user_id, host='127.0.0.1', port=None):
        self.user_id = user_id.lower()
        self.host = host
        
        # Assign port based on user_id
        if port is None:
            if self.user_id == 'alice':
                self.port = 5001
            elif self.user_id == 'bob':
                self.port = 5002
            elif self.user_id == 'charlie':
                self.port = 5003
            elif self.user_id == 'dave':
                self.port = 5004
            else:
                raise ValueError(f"Unknown user_id: {user_id}")
        else:
            self.port = port
        
        # Initialize blockchain
        self.blockchain = Blockchain(self.user_id)
        
        # Peer information
        self.peers = {
            'alice': ('127.0.0.1', 5001),
            'bob': ('127.0.0.1', 5002),
            'charlie': ('127.0.0.1', 5003),
            'dave': ('127.0.0.1', 5004)
        }
        
        # Remove self from peers
        if self.user_id in self.peers:
            del self.peers[self.user_id]
        
        # Initialize server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Lock for thread safety
        self.lock = threading.Lock()
        
        # Connected peers
        self.connected_peers = {}
    
    def start(self):
        """Start the blockchain peer."""
        # Start server
        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            print(f"{self.user_id.capitalize()} node started on {self.host}:{self.port}")
            
            # Start server thread
            server_thread = threading.Thread(target=self.listen_for_connections)
            server_thread.daemon = True
            server_thread.start()
            
            # Connect to peers
            self.connect_to_peers()
            
            # Synchronize blockchain with peers
            self.synchronize_blockchain()
            
            # Ask for initial balance if not set
            self.check_initial_balance()
            
            # Start main menu
            self.main_menu()
        
        except Exception as e:
            print(f"Error starting peer: {e}")
        finally:
            self.server_socket.close()
    
    def listen_for_connections(self):
        """Listen for incoming connections from other peers."""
        try:
            while True:
                client_socket, address = self.server_socket.accept()
                print(f"Connection from {address}")
                
                # Start a new thread to handle the client
                client_thread = threading.Thread(target=self.handle_peer_connection, args=(client_socket,))
                client_thread.daemon = True
                client_thread.start()
        except Exception as e:
            print(f"Server error: {e}")
    
    def handle_peer_connection(self, client_socket):
        """Handle incoming peer connections."""
        try:
            # Get peer identification
            data = client_socket.recv(1024).decode('utf-8')
            peer_data = json.loads(data)
            peer_id = peer_data.get('peer_id', '').lower()
            
            if not peer_id or peer_id not in self.peers:
                response = {
                    'status': 'error',
                    'message': 'Invalid peer ID'
                }
                client_socket.send(json.dumps(response).encode('utf-8'))
                client_socket.close()
                return
            
            # Register the peer
            with self.lock:
                self.connected_peers[peer_id] = client_socket
            
            # Send acknowledgment
            response = {
                'status': 'connected',
                'message': f"Connected to {self.user_id}"
            }
            client_socket.send(json.dumps(response).encode('utf-8'))
            
            # Handle peer messages
            while True:
                data = client_socket.recv(4096).decode('utf-8')
                if not data:
                    break
                
                message = json.loads(data)
                self.process_peer_message(message, peer_id, client_socket)
        
        except Exception as e:
            print(f"Error handling peer connection: {e}")
        finally:
            # Clean up when peer disconnects
            with self.lock:
                if peer_id in self.connected_peers:
                    del self.connected_peers[peer_id]
            client_socket.close()
            print(f"Connection from {peer_id} closed")
    
    def connect_to_peers(self):
        """Connect to other peers."""
        for peer_id, (host, port) in self.peers.items():
            if peer_id not in self.connected_peers:
                try:
                    # Create socket
                    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    peer_socket.connect((host, port))
                    
                    # Send identification
                    peer_data = {
                        'peer_id': self.user_id
                    }
                    peer_socket.send(json.dumps(peer_data).encode('utf-8'))
                    
                    # Get response
                    response = json.loads(peer_socket.recv(1024).decode('utf-8'))
                    
                    if response.get('status') == 'connected':
                        with self.lock:
                            self.connected_peers[peer_id] = peer_socket
                        print(f"Connected to {peer_id}")
                        
                        # Start a thread to listen for messages from this peer
                        listener_thread = threading.Thread(target=self.listen_to_peer, args=(peer_socket, peer_id))
                        listener_thread.daemon = True
                        listener_thread.start()
                    else:
                        print(f"Failed to connect to {peer_id}: {response.get('message', 'Unknown error')}")
                        peer_socket.close()
                
                except Exception as e:
                    print(f"Error connecting to {peer_id}: {e}")
    
    def listen_to_peer(self, peer_socket, peer_id):
        """Listen for messages from a connected peer."""
        try:
            while True:
                data = peer_socket.recv(4096).decode('utf-8')
                if not data:
                    break
                
                message = json.loads(data)
                self.process_peer_message(message, peer_id, peer_socket)
        
        except Exception as e:
            print(f"Error listening to {peer_id}: {e}")
        finally:
            # Clean up when connection is lost
            with self.lock:
                if peer_id in self.connected_peers:
                    del self.connected_peers[peer_id]
            peer_socket.close()
            print(f"Connection to {peer_id} closed")
    
    def process_peer_message(self, message, peer_id, peer_socket):
        """Process messages received from peers."""
        message_type = message.get('type')
        
        if message_type == 'transaction':
            # Process a new transaction from a peer
            block_dict = message.get('block')
            
            if block_dict:
                with self.lock:
                    success = self.blockchain.add_block_from_peer(block_dict)
                
                if success:
                    print(f"\nReceived new transaction from {peer_id}")
                    print(f"Transaction Hash: {block_dict['hash']}")
                    
                    # Broadcast to other peers
                    self.broadcast_transaction(block_dict, exclude=peer_id)
        
        elif message_type == 'get_chain':
            # Send our blockchain to the requesting peer
            response = {
                'type': 'chain',
                'chain': [block.to_dict() for block in self.blockchain.chain]
            }
            peer_socket.send(json.dumps(response).encode('utf-8'))
        
        elif message_type == 'chain':
            # Received a blockchain from a peer
            peer_chain = message.get('chain', [])
            
            # Check if the peer's chain is longer than ours
            if len(peer_chain) > len(self.blockchain.chain):
                with self.lock:
                    # Try to resolve conflicts
                    self.blockchain.resolve_conflicts([peer_chain])
    
    def synchronize_blockchain(self):
        """Synchronize blockchain with peers."""
        # Request blockchain from all connected peers
        for peer_id, peer_socket in list(self.connected_peers.items()):
            try:
                request = {
                    'type': 'get_chain'
                }
                peer_socket.send(json.dumps(request).encode('utf-8'))
            except Exception as e:
                print(f"Error requesting chain from {peer_id}: {e}")
                # Remove disconnected peer
                with self.lock:
                    if peer_id in self.connected_peers:
                        del self.connected_peers[peer_id]
    
    def broadcast_transaction(self, block_dict, exclude=None):
        """Broadcast a transaction to all connected peers."""
        message = {
            'type': 'transaction',
            'block': block_dict
        }
        
        for peer_id, peer_socket in list(self.connected_peers.items()):
            if exclude and peer_id == exclude:
                continue
                
            try:
                peer_socket.send(json.dumps(message).encode('utf-8'))
            except Exception as e:
                print(f"Error broadcasting to {peer_id}: {e}")
                # Remove disconnected peer
                with self.lock:
                    if peer_id in self.connected_peers:
                        del self.connected_peers[peer_id]
    
    def check_initial_balance(self):
        """Check if initial balance is set and ask for it if not."""
        balance = self.blockchain.get_balance()
        
        if balance == 0:
            try:
                print(f"\nWelcome {self.user_id.capitalize()}!")
                initial_balance = float(input("Enter your initial balance in rupees: "))
                
                with self.lock:
                    # Set initial balance
                    self.blockchain.set_initial_balance(initial_balance)
                
                # Create a block for the initial balance
                block = self.blockchain.chain[-1]  # Get the last block (initial balance block)
                
                # Broadcast to peers
                self.broadcast_transaction(block.to_dict())
                
                print(f"Initial balance set to {initial_balance} rupees")
            except ValueError:
                print("Invalid balance. Please enter a valid number.")
                self.check_initial_balance()  # Try again
    
    def send_transaction(self):
        """Send a transaction to another user."""
        recipient = input("Enter recipient's name: ").lower()
        
        if recipient not in self.blockchain.users:
            print(f"Invalid recipient. Valid users are: {', '.join(self.blockchain.users)}")
            return
        
        if recipient == self.user_id:
            print("You cannot send money to yourself.")
            return
        
        try:
            amount = float(input("Enter amount to send: "))
            
            if amount <= 0:
                print("Amount must be greater than zero.")
                return
            
            with self.lock:
                # Add transaction to blockchain
                block = self.blockchain.add_transaction(self.user_id, recipient, amount)
            
            # Broadcast transaction to peers
            self.broadcast_transaction(block.to_dict())
            
            print(f"Transaction successful! Sent {amount} rupees to {recipient}")
            print(f"Transaction Hash: {block.hash}")
        
        except ValueError as e:
            print(f"Transaction failed: {e}")
    
    def view_transaction_history(self):
        """View transaction history."""
        history = self.blockchain.get_user_transaction_history()
        
        if not history:
            print("No transactions found.")
            return
        
        print(f"\nTransaction History for {self.user_id.capitalize()}:")
        print("=" * 50)
        
        for tx in history:
            if tx['type'] == 'initial_balance':
                print(f"Initial balance set to {tx['amount']} rupees on {tx['timestamp']}")
                print(f"Transaction Hash: {tx['block_hash']}")
            elif tx['type'] == 'sent':
                print(f"Sent {tx['amount']} rupees to {tx['to']} on {tx['timestamp']}")
                print(f"Transaction Hash: {tx['block_hash']}")
            else:
                print(f"Received {tx['amount']} rupees from {tx['from']} on {tx['timestamp']}")
                print(f"Transaction Hash: {tx['block_hash']}")
            print("-" * 50)
    
    def check_balance(self):
        """Check current balance."""
        balance = self.blockchain.get_balance()
        print(f"Your current balance is {balance} rupees")
    
    def verify_blockchain(self):
        """Verify the integrity of the blockchain."""
        is_valid = self.blockchain.is_chain_valid()
        print(f"Blockchain validity: {is_valid}")
    
    def main_menu(self):
        """Display main menu and handle user input."""
        while True:
            print("\n" + "=" * 50)
            print(f"{self.user_id.capitalize()}'s Blockchain Node")
            print("=" * 50)
            print(f"Current Balance: {self.blockchain.get_balance()} rupees")
            print(f"Connected Peers: {', '.join(self.connected_peers.keys()) if self.connected_peers else 'None'}")
            print("1. Send Transaction")
            print("2. View Transaction History")
            print("3. Check Balance")
            print("4. Verify Blockchain")
            print("5. Synchronize with Peers")
            print("6. Exit")
            print("=" * 50)
            
            choice = input("Enter your choice (1-6): ")
            
            if choice == '1':
                self.send_transaction()
            elif choice == '2':
                self.view_transaction_history()
            elif choice == '3':
                self.check_balance()
            elif choice == '4':
                self.verify_blockchain()
            elif choice == '5':
                self.synchronize_blockchain()
                print("Blockchain synchronized with peers")
            elif choice == '6':
                print("Exiting...")
                # Close all connections
                for peer_socket in self.connected_peers.values():
                    try:
                        peer_socket.close()
                    except:
                        pass
                break
            else:
                print("Invalid choice. Please try again.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python peer.py <user_id>")
        sys.exit(1)
    
    user_id = sys.argv[1]
    if user_id.lower() not in ['alice', 'bob', 'charlie', 'dave']:
        print("Invalid user ID. Must be one of: Alice, Bob, Charlie, Dave")
        sys.exit(1)
    
    peer = BlockchainPeer(user_id)
    peer.start()

