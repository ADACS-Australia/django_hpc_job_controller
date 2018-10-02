import os

# The working directory is the directory used by the daemon for writing log files
HPC_LOG_DIRECTORY = os.path.join(os.getcwd(), 'log')

# The path to the pid file of the daemon
HPC_DAEMON_PID_FILE = '/tmp/job_controller.pid'

# The remote web address of the websocket server (ws(s)://host:port)
HPC_WEBSOCKET_SERVER = 'ws://localhost:8001'

# The scheduler class
HPC_SCHEDULER_CLASS = 'scheduler.slurm.Slurm'

# The location of job working directory
HPC_JOB_WORKING_DIRECTORY = '/tmp/jobs/'
