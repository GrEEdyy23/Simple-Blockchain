import sys
import os

# Create necessary directories
if not os.path.exists("logs"):
    os.makedirs("logs")
if not os.path.exists("data"):
    os.makedirs("data")
if not os.path.exists("users"):
    os.makedirs("users")

# Run Charlie's node
sys.argv = ["peer.py", "Charlie"]
from peer import BlockchainPeer

if __name__ == "__main__":
    peer = BlockchainPeer("Charlie")
    peer.start()

