import os

# The working directory is the directory used by the daemon for writing log files
LOG_DIRECTORY = os.path.join(os.getcwd(), 'log')

# The path to the pid file of the daemon
DAEMON_PID_FILE = '/tmp/job_controller.pid'

# The remote web address of the websocket server (ws(s)://host:port)
WEBSOCKET_SERVER = 'ws://localhost:8001'

# The size of each chunk to send over the file websocket
FILE_CONNECTION_CHUNK_SIZE = 1024*1024

try:
    from .local import *
except:
    pass