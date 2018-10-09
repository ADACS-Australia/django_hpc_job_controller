import sys

from filelock import FileLock

# Lock the /tmp lock file so that multiple
lock = FileLock("/tmp/hpc-job-controller-lock")

# Acquire the lock
with lock:
    # Check that the websocket server is running
    from .server.startup import check_websocket_server

    # Verify that this isn't a management command
    if ('manage' not in sys.argv[0] and 'manage' not in sys.argv[1]) or sys.argv[1] == 'runserver':
        # Start the server
        check_websocket_server()
