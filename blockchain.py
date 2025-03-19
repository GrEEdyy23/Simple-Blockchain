import json
import time
import hashlib
from typing import Dict, List
import os
import logging

class Block:
    def __init__(self, index: int, timestamp: float, transaction: Dict, previous_hash: str, logger=None):
        self.index = index
        self.timestamp = timestamp
        self.transaction = transaction
        self.previous_hash = previous_hash
        self.hash = self.calculate_hash()
        self.logger = logger
        
        if self.logger:
            self.logger.info(f"Created new block: {self.index} with hash: {self.hash}")
            self.logger.info(f"Block transaction: {self.transaction}")
    
    def calculate_hash(self) -> str:
        """Calculate the SHA-256 hash of the block contents."""
        block_string = json.dumps({
            "index": self.index,
            "timestamp": self.timestamp,
            "transaction": self.transaction,
            "previous_hash": self.previous_hash
        }, sort_keys=True).encode()
        
        return hashlib.sha256(block_string).hexdigest()
    
    def to_dict(self) -> Dict:
        """Convert block to dictionary for serialization."""
        return {
            "index": self.index,
            "timestamp": self.timestamp,
            "transaction": self.transaction,
            "previous_hash": self.previous_hash,
            "hash": self.hash
        }
    
    @classmethod
    def from_dict(cls, block_dict: Dict) -> 'Block':
        """Create a block from a dictionary."""
        block = cls(
            block_dict["index"],
            block_dict["timestamp"],
            block_dict["transaction"],
            block_dict["previous_hash"]
        )
        block.hash = block_dict["hash"]
        return block

class Blockchain:
    def __init__(self, user_id: str):
        self.user_id = user_id.lower()
        self.chain = []
        self.users = ["alice", "bob", "charlie", "dave"]
        self.pending_transactions = []
        
        # Set up logging first
        self.setup_logging()
        
        # Load blockchain if it exists
        blockchain_file = f"data/{self.user_id}_blockchain.json"
        if os.path.exists(blockchain_file):
            self.load_from_file(blockchain_file)
        else:
            # Create genesis block
            self.create_genesis_block()
        
        # Create user files if they don't exist
        self.create_user_files()
    
    def setup_logging(self):
        """Set up logging for the blockchain."""
        if not os.path.exists("logs"):
            os.makedirs("logs")
        
        if not os.path.exists("data"):
            os.makedirs("data")
        
        self.logger = logging.getLogger(f"blockchain_{self.user_id}")
        self.logger.setLevel(logging.INFO)
        
        # Create file handler
        log_file = f"logs/{self.user_id}_blockchain.log"
        file_handler = logging.FileHandler(log_file)
        
        # Create formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        
        # Add handler to logger
        self.logger.addHandler(file_handler)
        
        # Create transaction log file with headers
        with open(f"logs/{self.user_id}_transactions.log", "w") as f:
            f.write("Timestamp,Transaction Hash,Sender,Recipient,Amount,Alice Balance,Bob Balance,Charlie Balance,Dave Balance\n")
    
    def create_user_files(self):
        """Create individual files for each user."""
        if not os.path.exists("users"):
            os.makedirs("users")
        
        for user in self.users:
            user_file = f"users/{user}.json"
            if not os.path.exists(user_file):
                user_data = {
                    "name": user.capitalize(),
                    "balance": 0,  # Initial balance will be set by user
                    "transactions": []
                }
                with open(user_file, 'w') as f:
                    json.dump(user_data, f, indent=2)
                self.logger.info(f"Created user file for {user}")
    
    def create_genesis_block(self):
        """Create the first block in the blockchain."""
        genesis_block = Block(0, time.time(), {
            "sender": "genesis",
            "recipient": "genesis",
            "amount": 0
        }, "0", self.logger)
        self.chain.append(genesis_block)
        self.logger.info(f"Genesis block created with hash: {genesis_block.hash}")
        
        # Save blockchain
        self.save_to_file(f"data/{self.user_id}_blockchain.json")
    
    def get_latest_block(self) -> Block:
        """Return the most recent block in the chain."""
        return self.chain[-1]
    
    def is_transaction_valid(self, transaction: Dict) -> bool:
        """Check if a transaction is valid based on sender's balance."""
        sender = transaction["sender"].lower()
        amount = transaction["amount"]
        
        # Skip validation for genesis transactions
        if sender == "genesis":
            return True
        
        # Skip validation for initial balance setting
        if sender == "initial_balance":
            return True
        
        # Get current balances
        balances = self.calculate_balances()
        
        # Check if sender has enough balance
        if sender in balances and balances[sender] >= amount:
            self.logger.info(f"Transaction valid: {sender} has sufficient balance ({balances[sender]} rupees) for {amount} rupees transfer")
            return True
        else:
            self.logger.warning(f"Transaction invalid: {sender} has insufficient balance ({balances.get(sender, 0)} rupees) for {amount} rupees transfer")
            return False
    
    def add_transaction(self, sender: str, recipient: str, amount: float) -> Block:
        """Create a transaction and add it to the blockchain if valid."""
        transaction = {
            "sender": sender.lower(),
            "recipient": recipient.lower(),
            "amount": amount
        }
        
        # Validate transaction first
        if not self.is_transaction_valid(transaction):
            self.logger.error(f"Failed to add transaction: Insufficient funds")
            raise ValueError("Transaction invalid: Insufficient funds")
        
        # Create a new block with the transaction
        previous_block = self.get_latest_block()
        new_index = previous_block.index + 1
        new_timestamp = time.time()
        new_block = Block(new_index, new_timestamp, transaction, previous_block.hash, self.logger)
        
        self.chain.append(new_block)
        self.logger.info(f"Added block {new_index} with hash: {new_block.hash}")
        
        # Update transaction log
        self.update_transaction_log(new_block)
        
        # Update user files
        self.update_user_files(new_block)
        
        # Display balance table after transaction
        self.display_balance_table()
        
        # Save blockchain
        self.save_to_file(f"data/{self.user_id}_blockchain.json")
        
        return new_block
    
    def set_initial_balance(self, balance: float):
        """Set the initial balance for the user."""
        # Create a special transaction for initial balance
        transaction = {
            "sender": "initial_balance",
            "recipient": self.user_id,
            "amount": balance
        }
        
        # Create a new block with the transaction
        previous_block = self.get_latest_block()
        new_index = previous_block.index + 1
        new_timestamp = time.time()
        new_block = Block(new_index, new_timestamp, transaction, previous_block.hash, self.logger)
        
        self.chain.append(new_block)
        self.logger.info(f"Added initial balance block {new_index} with hash: {new_block.hash}")
        
        # Update user file
        user_file = f"users/{self.user_id}.json"
        if os.path.exists(user_file):
            with open(user_file, 'r') as f:
                user_data = json.load(f)
            
            # Set balance
            user_data["balance"] = balance
            
            # Write updated data back to file
            with open(user_file, 'w') as f:
                json.dump(user_data, f, indent=2)
            
            self.logger.info(f"Set initial balance for {self.user_id} to {balance}")
        
        # Save blockchain
        self.save_to_file(f"data/{self.user_id}_blockchain.json")
        
        # Display balance table
        self.display_balance_table()
    
    def update_transaction_log(self, block: Block):
        """Update the transaction log file with the new transaction."""
        transaction = block.transaction
        balances = self.calculate_balances()
        
        with open(f"logs/{self.user_id}_transactions.log", "a") as f:
            timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(block.timestamp))
            f.write(f"{timestamp},{block.hash},{transaction['sender']},{transaction['recipient']},{transaction['amount']},"
                   f"{balances.get('alice', 0)},{balances.get('bob', 0)},{balances.get('charlie', 0)},{balances.get('dave', 0)}\n")
    
    def update_user_files(self, block: Block):
        """Update the individual user files with the new transaction."""
        transaction = block.transaction
        sender = transaction["sender"].lower()
        recipient = transaction["recipient"].lower()
        amount = transaction["amount"]
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(block.timestamp))
        
        # Skip genesis transactions
        if sender == "genesis" and recipient == "genesis":
            return
        
        # Handle initial balance setting
        if sender == "initial_balance":
            self.update_user_file(recipient, amount, {
                "type": "initial_balance",
                "amount": amount,
                "timestamp": timestamp,
                "block_hash": block.hash
            })
            return
        
        # Update sender's file if it's one of our users
        if sender in self.users:
            self.update_user_file(sender, -amount, {
                "type": "sent",
                "to": recipient,
                "amount": amount,
                "timestamp": timestamp,
                "block_hash": block.hash
            })
        
        # Update recipient's file if it's one of our users
        if recipient in self.users:
            self.update_user_file(recipient, amount, {
                "type": "received",
                "from": sender,
                "amount": amount,
                "timestamp": timestamp,
                "block_hash": block.hash
            })
    
    def update_user_file(self, user: str, amount_change: float, transaction_data: Dict):
        """Update a specific user's file with transaction data."""
        user_file = f"users/{user}.json"
        
        if os.path.exists(user_file):
            with open(user_file, 'r') as f:
                user_data = json.load(f)
            
            # Update balance for normal transactions
            if transaction_data["type"] != "initial_balance":
                user_data["balance"] += amount_change
            else:
                # For initial balance, set the balance directly
                user_data["balance"] = amount_change
            
            # Add transaction to history
            user_data["transactions"].append(transaction_data)
            
            # Write updated data back to file
            with open(user_file, 'w') as f:
                json.dump(user_data, f, indent=2)
            
            self.logger.info(f"Updated user file for {user}")
    
    def is_chain_valid(self) -> bool:
        """Verify the integrity of the blockchain."""
        for i in range(1, len(self.chain)):
            current_block = self.chain[i]
            previous_block = self.chain[i-1]
            
            # Check if the hash of the block is correct
            if current_block.hash != current_block.calculate_hash():
                self.logger.warning(f"Block {current_block.index} has invalid hash")
                return False
            
            # Check if the previous hash reference is correct
            if current_block.previous_hash != previous_block.hash:
                self.logger.warning(f"Block {current_block.index} has invalid previous hash reference")
                return False
        
        self.logger.info("Blockchain validated successfully")
        return True
    
    def calculate_balances(self) -> Dict[str, float]:
        """Calculate the current balance of each user."""
        balances = {
            "alice": 0,
            "bob": 0,
            "charlie": 0,
            "dave": 0
        }
        
        for block in self.chain[1:]:  # Skip genesis block
            transaction = block.transaction
            sender = transaction["sender"].lower()
            recipient = transaction["recipient"].lower()
            amount = transaction["amount"]
            
            # Handle initial balance setting
            if sender == "initial_balance":
                if recipient in balances:
                    balances[recipient] = amount
                continue
            
            if sender in balances:
                balances[sender] -= amount
            
            if recipient in balances:
                balances[recipient] += amount
        
        return balances
    
    def get_balance(self, user_id: str = None) -> float:
        """Get the balance of a specific user."""
        if user_id is None:
            user_id = self.user_id
        
        balances = self.calculate_balances()
        return balances.get(user_id.lower(), 0)
    
    def display_balance_table(self):
        """Display a table of current balances for all users."""
        balances = self.calculate_balances()
        
        self.logger.info("Current Balance Table:")
        self.logger.info("=============================================")
        self.logger.info("| User     | Balance (Rupees)               |")
        self.logger.info("=============================================")
        for user, balance in balances.items():
            self.logger.info(f"| {user.ljust(8)} | {str(balance).ljust(28)} |")
        self.logger.info("=============================================")
        
        # Also print to console for immediate feedback
        print("\nCurrent Balance Table:")
        print("=============================================")
        print("| User     | Balance (Rupees)               |")
        print("=============================================")
        for user, balance in balances.items():
            print(f"| {user.ljust(8)} | {str(balance).ljust(28)} |")
        print("=============================================\n")
    
    def to_dict(self) -> Dict:
        """Convert blockchain to dictionary for serialization."""
        return {
            "chain": [block.to_dict() for block in self.chain]
        }
    
    def from_dict(self, blockchain_dict: Dict):
        """Load blockchain from dictionary."""
        self.chain = [Block.from_dict(block_dict) for block_dict in blockchain_dict["chain"]]
        self.logger.info(f"Loaded blockchain with {len(self.chain)} blocks")
    
    def save_to_file(self, filename: str):
        """Save blockchain to a file."""
        with open(filename, 'w') as f:
            json.dump(self.to_dict(), f)
        self.logger.info(f"Saved blockchain to {filename}")
    
    def load_from_file(self, filename: str):
        """Load blockchain from a file."""
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                blockchain_dict = json.load(f)
            self.from_dict(blockchain_dict)
            self.logger.info(f"Loaded blockchain from {filename}")
        else:
            self.logger.warning(f"Blockchain file {filename} not found")
            # Create genesis block if file doesn't exist
            self.create_genesis_block()
    
    def get_user_transaction_history(self, user: str = None) -> List[Dict]:
        """Get the transaction history for a specific user."""
        if user is None:
            user = self.user_id
            
        user_file = f"users/{user.lower()}.json"
        
        if os.path.exists(user_file):
            with open(user_file, 'r') as f:
                user_data = json.load(f)
            return user_data["transactions"]
        else:
            self.logger.warning(f"User file for {user} not found")
            return []
    
    def add_block_from_peer(self, block_dict: Dict) -> bool:
        """Add a block received from a peer to the blockchain."""
        # Convert dict to Block object
        block = Block.from_dict(block_dict)
        
        # Verify the block
        if len(self.chain) > 0:
            latest_block = self.get_latest_block()
            
            # Check if the block is the next in sequence
            if block.index != latest_block.index + 1:
                self.logger.warning(f"Block index mismatch: expected {latest_block.index + 1}, got {block.index}")
                return False
            
            # Check if previous hash matches
            if block.previous_hash != latest_block.hash:
                self.logger.warning(f"Previous hash mismatch: expected {latest_block.hash}, got {block.previous_hash}")
                return False
            
            # Verify transaction
            if not self.is_transaction_valid(block.transaction):
                self.logger.warning(f"Transaction in block {block.index} is invalid")
                return False
        
        # Add the block to the chain
        self.chain.append(block)
        self.logger.info(f"Added block {block.index} from peer with hash: {block.hash}")
        
        # Update transaction log
        self.update_transaction_log(block)
        
        # Update user files
        self.update_user_files(block)
        
        # Save blockchain
        self.save_to_file(f"data/{self.user_id}_blockchain.json")
        
        # Display balance table
        self.display_balance_table()
        
        return True
    
    def resolve_conflicts(self, peer_chains: List[List[Dict]]) -> bool:
        """
        Consensus algorithm to resolve conflicts between blockchain copies.
        Chooses the longest valid chain.
        """
        max_length = len(self.chain)
        new_chain = None
        
        # Look for chains longer than ours
        for peer_chain_dict in peer_chains:
            peer_chain = [Block.from_dict(block_dict) for block_dict in peer_chain_dict]
            length = len(peer_chain)
            
            # Check if the chain is longer and valid
            if length > max_length:
                # Verify the chain
                valid = True
                for i in range(1, length):
                    if peer_chain[i].previous_hash != peer_chain[i-1].hash:
                        valid = False
                        break
                    if peer_chain[i].hash != peer_chain[i].calculate_hash():
                        valid = False
                        break
                
                if valid:
                    max_length = length
                    new_chain = peer_chain
        
        # Replace our chain if we found a longer valid one
        if new_chain:
            self.chain = new_chain
            self.logger.info(f"Replaced chain with longer valid chain of length {max_length}")
            
            # Update user files based on new chain
            self.update_all_user_files()
            
            # Save blockchain
            self.save_to_file(f"data/{self.user_id}_blockchain.json")
            
            return True
        
        return False
    
    def update_all_user_files(self):
        """Update all user files based on the current blockchain."""
        # Reset user files
        for user in self.users:
            user_file = f"users/{user}.json"
            user_data = {
                "name": user.capitalize(),
                "balance": 0,
                "transactions": []
            }
            with open(user_file, 'w') as f:
                json.dump(user_data, f, indent=2)
        
        # Replay all transactions
        for block in self.chain[1:]:  # Skip genesis block
            self.update_user_files(block)
        
        self.logger.info("Updated all user files based on current blockchain")

