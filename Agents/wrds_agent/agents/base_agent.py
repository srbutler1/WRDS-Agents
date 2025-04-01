"""Base agent class for WRDS Multi-Agent System.

This module provides a base class for all agents in the system.
"""

import logging
import os
import uuid
from typing import Dict, Any, List, Optional

class Message:
    """A message passed between agents."""
    def __init__(self, sender: str, content: Dict[str, Any], message_type: str, receiver: str = None):
        self.id = str(uuid.uuid4())
        self.sender = sender
        self.receiver = receiver
        self.content = content
        self.message_type = message_type
        
    def __str__(self):
        return f"Message(id={self.id}, type={self.message_type}, sender={self.sender}, receiver={self.receiver})"

class BaseAgent:
    """Base class for all agents in the system."""
    
    def __init__(self, agent_name: str):
        self.agent_name = agent_name
        self.logger = logging.getLogger(f"{agent_name}")
        self.message_queue: List[Message] = []
        self.connections: Dict[str, 'BaseAgent'] = {}
        
    def connect(self, agent_name: str, agent: 'BaseAgent'):
        """Connect this agent to another agent."""
        self.connections[agent_name] = agent
        self.logger.info(f"Connected to agent: {agent_name}")
        
    def send_message(self, receiver: str, content: Dict[str, Any], message_type: str) -> str:
        """Send a message to another agent.
        
        Args:
            receiver: Name of the receiving agent
            content: Message content as a dictionary
            message_type: Type of message (e.g., 'request', 'response')
            
        Returns:
            Message ID
        """
        if receiver not in self.connections:
            self.logger.error(f"Cannot send message to unknown agent: {receiver}")
            return ""
            
        message = Message(self.agent_name, content, message_type, receiver)
        self.logger.info(f"Sending message: {message}")
        self.connections[receiver].receive_message(message)
        return message.id
        
    def receive_message(self, message: Message):
        """Receive a message from another agent.
        
        Args:
            message: The message received
        """
        self.logger.info(f"Received message: {message}")
        self.message_queue.append(message)
        self.process_messages()
        
    def process_messages(self):
        """Process all messages in the queue."""
        while self.message_queue:
            message = self.message_queue.pop(0)
            self.process_message(message)
            
    def process_message(self, message: Message):
        """Process a single message.
        
        Args:
            message: The message to process
        """
        self.logger.info(f"Processing message: {message}")
        # To be implemented by subclasses
        
    def handle_request(self, message: Message) -> Optional[Dict[str, Any]]:
        """Handle a request message.
        
        Args:
            message: The request message
            
        Returns:
            Response content or None if no response is needed
        """
        # To be implemented by subclasses
        return None

    def _get_valid_api_key(self) -> str:
        """Get a valid OpenAI API key or return a dummy key for testing.
        
        Returns:
            A valid API key or a dummy key for testing
        """
        api_key = os.getenv("OPENAI_API_KEY")
        
        # Check if API key is valid (should start with 'sk-')
        if not api_key or not api_key.startswith("sk-"):
            self.logger.warning("Invalid or missing OpenAI API key. Using dummy key for testing.")
            # Use a dummy key for testing that has the correct format
            api_key = "sk-dummy1234567890abcdefghijklmnopqrstuvwxyz1234567890abcdefgh"
        
        return api_key
