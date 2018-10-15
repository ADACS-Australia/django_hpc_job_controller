import os


class Scheduler:
    """
    Base stub scheduler - this class should be inherited and extended
    """

    def __init__(self, settings, ui_id, job_id):
        """
        Initialises the job scheduler

        :param ui_id: The UI ID of the job
        :param job_id: The scheduler job id
        """
        self.ui_id = ui_id
        self.job_id = job_id
        self.settings = settings

    def get_working_directory(self):
        """
        Returns the working directory for this job

        :return: The working directory for this job
        """
        return os.path.join(self.settings.HPC_JOB_WORKING_DIRECTORY, str(self.ui_id))

    def _submit(self, job_parameters):
        """
        Used to submit a job on the cluster

        Entry to the submit function. This function is called by the job controller. Override this with any before/after
        job submission logic specific to the scheduler. Call submit from this function

        :param job_parameters: The job parameters for this job
        :return: An integer identifier for the submitted job
        """
        raise NotImplementedError()

    def submit(self, job_parameters):
        """
        Used to submit a job on the cluster

        :param job_parameters: The job parameters for this job
        :return: An integer identifier for the submitted job
        """
        raise NotImplementedError()

    def status(self):
        """
        Get the status of a job

        :return: A tuple with JobStatus, additional info as a string
        """
        raise NotImplementedError()

    def cancel(self):
        """
        Cancel a running job

        :param job_id: The id of the job to cancel
        :return: Nothing
        """
        raise NotImplementedError()

    def delete_data(self):
        """
        Delete all job data

        :return: Nothing
        """
        raise NotImplementedError()
