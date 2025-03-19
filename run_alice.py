import sys
import os

# Create necessary directories
if not os.path.exists("logs"):
    os.makedirs("logs")
if not os.path.exists("data"):
    os.makedirs("data")
if not os.path.exists("users"):
    os.makedirs("users")

# Run Alice's node
sys.argv = ["peer.py", "Alice"]
from peer import BlockchainPeer

if __name__ == "__main__":
    peer = BlockchainPeer("Alice")
    peer.start()

