import asyncio
import threading
import datetime
from mitmproxy import http
from mitmproxy.addons import default_addons, script
from mitmproxy.master import Master
from mitmproxy.options import Options

class HTTPMonitor:
    def __init__(self):
        self.count = 0

    def request(self, flow: http.HTTPFlow):
        self.count += 1
        print(f"Request #{self.count}: {flow.request.method} {flow.request.pretty_url}")

    def response(self, flow: http.HTTPFlow):
        # Calculate response time
        response_time = (flow.response.timestamp_end - flow.request.timestamp_start) * 1000

        # Format timestamps
        start_time = datetime.datetime.fromtimestamp(flow.request.timestamp_start).isoformat()
        end_time = datetime.datetime.fromtimestamp(flow.response.timestamp_end).isoformat()

        # Extract various headers and metadata
        range_header = flow.request.headers.get('Range', '')
        user_agent = flow.request.headers.get('User-Agent', '')[:100]  # Truncate long UAs
        content_type = flow.response.headers.get('Content-Type', '')
        
        # Calculate sizes
        request_size = len(flow.request.content) if flow.request.content else 0
        response_size = len(flow.response.content) if flow.response.content else 0

        # Store the data as a dictionary in a list
        response_dict = {
            "start_time": start_time,
            "end_time": end_time,
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

        print(response_dict)

class TrafficLogger(threading.Thread):
    def __init__(self, **options):
        self.loop = asyncio.new_event_loop()
        self.master = Master(Options(), event_loop=self.loop)
        
        # Add your custom addon
        self.master.addons.add(
            *[HTTPMonitor() if isinstance(addon, script.ScriptLoader) else addon 
              for addon in default_addons()]
        )
        
        # Configure options (like port)
        self.master.options.update(**options)
        super().__init__(daemon=True)

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.master.run())

    def shutdown(self):
        self.master.shutdown()
