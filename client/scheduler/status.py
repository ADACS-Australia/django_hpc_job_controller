class JobStatus:
    # A job is submitted if it cannot be immediately queued on a cluster (ie, all available clusters are offline)
    SUBMITTED = 0
    # A job is queued if it is in the queue on the cluster it is to run on
    QUEUED = 1
    # A job is running if it is currently running on the cluster it is to run on
    RUNNING = 2
    # A job is completed if it is finished running on the cluster without error
    COMPLETED = 3
    # A job is error if it crashed at any point during it's execution
    ERROR = 4
