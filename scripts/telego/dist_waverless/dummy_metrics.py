#测试用，生成假的 Prometheus 指标
from http.server import BaseHTTPRequestHandler, HTTPServer

class MetricsHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            # 这里写入一个假的 Prometheus 指标
            self.wfile.write(b'# HELP dummy_metric A dummy metric\n# TYPE dummy_metric counter\ndummy_metric 1\n')
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == '__main__':
    server = HTTPServer(('0.0.0.0', 2500), MetricsHandler)
    print('Serving /metrics on port 2500...')
    server.serve_forever()