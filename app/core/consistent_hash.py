import hashlib
from typing import List, Dict, Any
from bisect import bisect

class ConsistentHash:
    def __init__(self, nodes: List[str], virtual_nodes: int = 100):
        """
        Initialize the consistent hash ring
        
        Args:
            nodes: List of node identifiers (parsed from comma-separated string)
            virtual_nodes: Number of virtual nodes per physical node
        """
        self.virtual_nodes = virtual_nodes
        self.ring = {}  # Hash value -> node mapping
        self.sorted_keys = []  # Sorted list of hash values
        self.nodes = set()  # Set of actual nodes
        
        # Add nodes to the ring
        for node in nodes:
            self.add_node(node)
    
    def add_node(self, node: str) -> None:
        """
        Add a node to the consistent hash ring
        
        Args:
            node: Node identifier
        """
        if node in self.nodes:
            return
        
        self.nodes.add(node)
        
        # Add virtual nodes
        for i in range(self.virtual_nodes):
            key = f"{node}:{i}"
            hash_value = self._hash(key)
            self.ring[hash_value] = node
            self.sorted_keys.append(hash_value)
        
        # Sort the keys
        self.sorted_keys.sort()
    
    def remove_node(self, node: str) -> None:
        """
        Remove a node from the consistent hash ring
        
        Args:
            node: Node identifier
        """
        if node not in self.nodes:
            return
        
        self.nodes.remove(node)
        
        # Remove virtual nodes
        for i in range(self.virtual_nodes):
            key = f"{node}:{i}"
            hash_value = self._hash(key)
            if hash_value in self.ring:
                del self.ring[hash_value]
                self.sorted_keys.remove(hash_value)
    
    def get_node(self, key: str) -> str:
        """
        Get the node responsible for the given key
        
        Args:
            key: The key to look up
            
        Returns:
            The node responsible for the key
        """
        if not self.ring:
            raise Exception("Hash ring is empty")
        
        # Calculate hash of the key
        hash_value = self._hash(key)
        
        # Find the first node in the ring that comes after the key's hash
        idx = bisect(self.sorted_keys, hash_value)
        
        # If no such node exists, wrap around to the first node
        if idx >= len(self.sorted_keys):
            idx = 0
        
        return self.ring[self.sorted_keys[idx]]
    
    def _hash(self, key: str) -> int:
        """
        Hash a key to an integer value
        
        Args:
            key: The key to hash
            
        Returns:
            Integer hash value
        """
        # Use MD5 for consistent hashing
        hash_obj = hashlib.md5(key.encode('utf-8'))
        return int(hash_obj.hexdigest(), 16)
    