import http.server, ssl, os, sys

os.chdir("/home/gongbojun/html")

_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
_ctx.load_cert_chain("/home/gongbojun/html/certs/cert.pem", "/home/gongbojun/html/certs/key.pem")


class Handler(http.server.SimpleHTTPRequestHandler):
    def setup(self):
        # 监听 socket 不包 TLS: accept() 只做纯 TCP, 主循环永不被握手卡住;
        # TLS 握手放到每个连接的线程里, 并加 10s 超时, 半开/异常连接超时后自动关闭回收。
        raw = self.request
        raw.settimeout(10)
        try:
            self.request = _ctx.wrap_socket(raw, server_side=True)
        except Exception:
            try:
                raw.close()
            except Exception:
                pass
            raise
        super().setup()
        self.connection.settimeout(60)  # 单个请求读超时: 空闲长连接不会永久占线程

    def log_message(self, fmt, *args):
        # 避免 log 触发反向 DNS(本网络 DNS 不通时会卡几秒)
        sys.stderr.write("%s %s - - %s\n"
                         % (self.log_date_time_string(), self.client_address[0], fmt % args))


class Server(http.server.ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True

    def handle_error(self, request, client_address):
        pass  # 连接层错误(握手超时/被断)不必打印


if __name__ == "__main__":
    srv = Server(("0.0.0.0", 8443), Handler)
    print("HTTPS serving /home/gongbojun/html on :8443 (threaded, per-conn handshake timeout)", flush=True)
    srv.serve_forever()
