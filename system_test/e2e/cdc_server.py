#!/usr/bin/env python
"""
Python HTTP server for CDC end-to-end testing.

This module provides an HTTP test server designed for end-to-end 
testing of CDC functionality.
"""

import json
import random
import socket
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from typing import Dict, List, Optional, Any


class CDCMessageEvent:
    """Represents a single CDC event within a CDC message."""
    
    def __init__(self, op: str, table: str = "", new_row_id: int = 0, 
                 old_row_id: int = 0, before: Optional[Dict[str, Any]] = None, 
                 after: Optional[Dict[str, Any]] = None):
        self.op = op
        self.table = table
        self.new_row_id = new_row_id
        self.old_row_id = old_row_id
        self.before = before or {}
        self.after = after or {}
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CDCMessageEvent':
        """Create CDCMessageEvent from dictionary."""
        return cls(
            op=data.get('op', ''),
            table=data.get('table', ''),
            new_row_id=data.get('new_row_id', 0),
            old_row_id=data.get('old_row_id', 0),
            before=data.get('before', {}),
            after=data.get('after', {})
        )


class CDCMessage:
    """Represents a single CDC message containing an index and a list of events."""
    
    def __init__(self, index: int, events: List[CDCMessageEvent]):
        self.index = index
        self.events = events
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CDCMessage':
        """Create CDCMessage from dictionary."""
        events = [CDCMessageEvent.from_dict(event) for event in data.get('events', [])]
        return cls(
            index=data.get('index', 0),
            events=events
        )


class CDCMessagesEnvelope:
    """The envelope for CDC messages as transported over HTTP."""
    
    def __init__(self, service_id: str = "", node_id: str = "", 
                 payload: Optional[List[CDCMessage]] = None, timestamp: int = 0):
        self.service_id = service_id
        self.node_id = node_id
        self.payload = payload or []
        self.timestamp = timestamp
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CDCMessagesEnvelope':
        """Create CDCMessagesEnvelope from dictionary."""
        payload = [CDCMessage.from_dict(msg) for msg in data.get('payload', [])]
        return cls(
            service_id=data.get('service_id', ''),
            node_id=data.get('node_id', ''),
            payload=payload,
            timestamp=data.get('ts_ns', 0)
        )


class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    """HTTP server that handles requests in separate threads."""
    daemon_threads = True


class CDCHTTPRequestHandler(BaseHTTPRequestHandler):
    """HTTP request handler for CDC messages."""
    
    def do_POST(self):
        """Handle POST requests."""
        server = self.server
        
        with server.lock:
            # Check if we should simulate a failure
            if server.fail_rate > 0 and random.randint(0, 99) < server.fail_rate:
                server.num_failed += 1
                self.send_error(503, "Service Unavailable")
                return
        
        # Read request body
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                self.send_error(400, "Bad Request")
                return
                
            body = self.rfile.read(content_length)
            if not body:
                self.send_error(400, "Bad Request")
                return
        except Exception:
            self.send_error(400, "Bad Request")
            return
        
        with server.lock:
            # Store the raw request
            server.requests.append(body)
            
            # Parse the CDC envelope
            try:
                data = json.loads(body.decode('utf-8'))
                envelope = CDCMessagesEnvelope.from_dict(data)
                
                # Store messages by index
                for msg in envelope.payload:
                    server.messages[msg.index] = msg
                    
                # Dump request if enabled
                if server.dump_request:
                    print(body.decode('utf-8'))
                    
            except (json.JSONDecodeError, UnicodeDecodeError, Exception):
                self.send_error(503, "Service Unavailable")
                return
        
        # Send success response
        self.send_response(200)
        self.end_headers()
    
    def log_message(self, format, *args):
        """Suppress default HTTP server logging."""
        pass


class HTTPTestServer:
    """
    HTTP test server that simulates an HTTP endpoint for receiving CDC messages.
    
    Designed for end-to-end testing of CDC functionality.
    """
    
    def __init__(self):
        """Initialize the HTTP test server."""
        self.requests: List[bytes] = []
        self.messages: Dict[int, CDCMessage] = {}
        self.lock = threading.Lock()
        self.fail_rate = 0  # Percentage of requests to fail (0-100)
        self.num_failed = 0
        self._dump_request = False
        
        # Create server with dynamic port allocation
        self.server = None
        self._find_free_port()
    
    @property
    def dump_request(self) -> bool:
        """Get dump_request setting."""
        return self._dump_request
    
    @dump_request.setter
    def dump_request(self, value: bool):
        """Set dump_request setting."""
        self._dump_request = value
        if self.server:
            self.server.dump_request = value
    
    def _find_free_port(self):
        """Find a free port and create the HTTP server."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(('127.0.0.1', 0))
            _, port = s.getsockname()
        
        self.server = ThreadingHTTPServer(('127.0.0.1', port), CDCHTTPRequestHandler)
        # Share server instance data with handler
        self.server.requests = self.requests
        self.server.messages = self.messages
        self.server.lock = self.lock
        self.server.fail_rate = self.fail_rate
        self.server.num_failed = 0
        self.server.dump_request = self._dump_request
    
    def url(self) -> str:
        """Return the URL of the test server."""
        if not self.server:
            raise RuntimeError("Server not initialized")
        host, port = self.server.server_address
        return f"http://{host}:{port}"
    
    def start(self):
        """Start the test server."""
        if not self.server:
            raise RuntimeError("Server not initialized")
        
        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()
    
    def close(self):
        """Stop and close the test server."""
        if self.server:
            try:
                self.server.shutdown()
            except:
                pass
            try:
                self.server.server_close()
            except:
                pass
            if hasattr(self, 'server_thread') and self.server_thread.is_alive():
                self.server_thread.join(timeout=1.0)
    
    def set_fail_rate(self, rate: int):
        """Set the percentage of requests that should fail (0-100)."""
        with self.lock:
            if rate < 0:
                rate = 0
            elif rate > 100:
                rate = 100
            self.fail_rate = rate
            if self.server:
                self.server.fail_rate = rate
    
    def get_requests(self) -> List[bytes]:
        """Return a copy of the requests received by the server."""
        with self.lock:
            return self.requests.copy()
    
    def get_request_count(self) -> int:
        """Return the number of requests received by the server."""
        with self.lock:
            return len(self.requests)
    
    def get_failed_request_count(self) -> int:
        """Return the number of requests that failed due to simulated failures."""
        with self.lock:
            return self.num_failed + (self.server.num_failed if self.server else 0)
    
    def get_highest_message_index(self) -> int:
        """Return the highest message index received by the server."""
        with self.lock:
            if not self.messages:
                return 0
            return max(self.messages.keys())
    
    def reset(self):
        """Delete all received data."""
        with self.lock:
            self.requests.clear()
            self.messages.clear()
            self.num_failed = 0
            if self.server:
                self.server.num_failed = 0
    
    def get_message_count(self) -> int:
        """Return the number of unique messages received by the server."""
        with self.lock:
            return len(self.messages)
    
    def check_messages_exist(self, n: int) -> bool:
        """
        Check if messages with indices from 1 to n exist in the server's stored messages.
        Return True if all messages exist, False otherwise.
        """
        with self.lock:
            for i in range(1, n + 1):
                if i not in self.messages:
                    return False
            return True