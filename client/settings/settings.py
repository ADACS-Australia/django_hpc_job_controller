import os

# The working directory is the directory used by the daemon for writing log files
HPC_LOG_DIRECTORY = os.path.join(os.getcwd(), 'log')

# The remote web address of the websocket server (ws(s)://host:port)
HPC_WEBSOCKET_SERVER = 'ws://127.0.0.1:8001'

# The scheduler class
HPC_SCHEDULER_CLASS = 'scheduler.local.Local'

# The location of job working directory
HPC_JOB_WORKING_DIRECTORY = '/tmp/jobs/'

try:
    from .local import *
except:
    pass