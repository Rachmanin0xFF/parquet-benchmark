import asyncio
import threading
import datetime
from mitmproxy import http
from mitmproxy.addons import default_addons
from mitmproxy.master import Master
from mitmproxy.options import Options
import queue

class HTTPMonitor:
    def __init__(self, event_queue=None):
        self.count = 0
        self.event_queue = event_queue

    def request(self, flow: http.HTTPFlow):
        self.count += 1
        if self.event_queue:
            self.event_queue.put({
                "type": "request",
                "method": flow.request.method,
                "url": flow.request.pretty_url
            })

    def response(self, flow: http.HTTPFlow):
        start_time = datetime.datetime.fromtimestamp(flow.request.timestamp_start)
        end_time = datetime.datetime.fromtimestamp(flow.response.timestamp_end)
        response_time = (flow.response.timestamp_end - flow.request.timestamp_start) * 1000  # Convert to ms
        
        # Get request/response sizes
        request_size = len(flow.request.raw_content) if flow.request.raw_content else 0
        response_size = len(flow.response.raw_content) if flow.response.raw_content else 0
        
        # Get headers and other info
        range_header = flow.request.headers.get("Range", "")
        user_agent = flow.request.headers.get("User-Agent", "")
        content_type = flow.response.headers.get("Content-Type", "")
        
        response_dict = {
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "response_time_ms": f"{response_time:.2f}",
            "method": flow.request.method,
            "url": flow.request.pretty_url,
            "status_code": flow.response.status_code,
            "range_header": range_header,
            "response_size": response_size,
            "request_size": request_size,
            "user_agent": user_agent,
            "content_type": content_type,
            "client_ip": flow.client_conn.address[0] if flow.client_conn.address else ""
        }
        
        if self.event_queue:
            self.event_queue.put({
                "type": "response",
                **response_dict
            })

class TrafficLogger(threading.Thread):
    def __init__(self, **options):
        self.loop = asyncio.new_event_loop()
        self.event_queue = queue.Queue()
        
        # Create the monitor with the queue
        self.monitor = HTTPMonitor(event_queue=self.event_queue)
        
        self.master = Master(Options(), event_loop=self.loop)
        
        # Add default addons AND your custom monitor
        self.master.addons.add(*default_addons())
        self.master.addons.add(self.monitor)
        
        # Configure options (like port)
        self.master.options.update(**options)
        super().__init__(daemon=True)

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.master.run())

    def shutdown(self):
        self.master.shutdown()
    
    def get_event(self, timeout=None):
        """Get an event from the traffic logger queue"""
        try:
            return self.event_queue.get(timeout=timeout)
        except queue.Empty:
            return None