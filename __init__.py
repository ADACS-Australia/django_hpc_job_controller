from filelock import FileLock

# Lock the /tmp lock file so that multiple
lock = FileLock("/tmp/hpc-job-controller-lock")

# Acquire the lock
with lock:
    # Check that the websocket server is running
    from .server.startup import check_websocket_server

    check_websocket_server()
