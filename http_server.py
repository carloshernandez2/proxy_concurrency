import http.server
import socketserver
from socketserver import ThreadingMixIn

class CustomHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

class ThreadedTCPServer(ThreadingMixIn, socketserver.TCPServer):
    pass

PORT = 9000

Handler = CustomHTTPRequestHandler
httpd = ThreadedTCPServer(("", PORT), Handler)

print(f"Serving on port {PORT}")
httpd.serve_forever()
