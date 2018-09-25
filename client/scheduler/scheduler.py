class Scheduler:
    """
    Base stub scheduler - this class should be inherited and extended
    """

    def submit(self):
        """
        Used to submit a job on the cluster

        :return: An integer identifier for the submitted job
        """
        raise NotImplementedError()

    def status(self, job_id):
        """
        Get the status of a job

        :param job_id: The id of the job to check
        :return: A tuple with JobStatus, additional info as a string
        """
        raise NotImplementedError()

    def cancel(self, job_id):
        """
        Cancel a running job

        :param job_id: The id of the job to cancel
        :return: True if the job was cancelled otherwise False
        """
        raise NotImplementedError()

    