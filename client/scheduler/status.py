class JobStatus:
    # A job that is currently in draft state - it is not yet submitted
    DRAFT = 0
    # A job is pending if it is currently waiting for a cluster to submit the job to
    PENDING = 1
    # A job is submitting if the job has been submitted but is waiting for the client to acknowledge it has received
    # the job submission command
    SUBMITTING = 2
    # A job is submitted if it cannot be immediately queued on a cluster (ie, all available clusters are offline)
    SUBMITTED = 3
    # A job is queued if it is in the queue on the cluster it is to run on
    QUEUED = 4
    # A job is running if it is currently running on the cluster it is to run on
    RUNNING = 5
    # A job is completed if it is finished running on the cluster without error
    COMPLETED = 6
    # A job is error if it crashed at any point during it's execution
    ERROR = 7
    # A job that has been deleted
    DELETED = 8
