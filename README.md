# proxy_concurrency

This is an example of how to use I/O multiplexing to create a proxy server that can handle multiple requests concurrently.

## How to run

1. Run the upstream server:

```bash
python3 http_server.py
```

2. Run a Clojure REPL on your favourite editor and evaluate the comments in the `proxy_concurrency.clj` file.

3. Open your browser and go to `http://localhost:8001/` to see the proxy server in action.

## License

MIT
