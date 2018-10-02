import os
import shutil
import uuid
from datetime import timedelta
from math import floor

from .scheduler import Scheduler


class Slurm(Scheduler):
    """
    Slurm stub scheduler - this class should be inherited and extended to provide custom business logic
    """

    def __init__(self, settings, ui_id, job_id):
        # Init the super class
        super().__init__(settings, ui_id, job_id)

        # Set the slurm template
        self.slurm_template = 'settings/slurm.sh.template'
        # Set the number of nodes
        self.nodes = 1
        # Set the number of tasks per node
        self.tasks_per_node = 1
        # Set the amount of ram in Mb per cpu
        self.memory = 100
        # Set the walltime in seconds
        self.walltime = 60
        # Set the job name
        self.job_name = str(uuid.uuid4())

    def generate_template_dict(self):
        """
        Generates a dictionary to pass to the string formatter for the slurm template.

        Should be overridden to add custom entries to the dictionary

        :return: The dictionary with the key/value pairs to add to render the slurm template
        """

        return {
            'nodes': self.nodes,
            'tasks_per_node': self.tasks_per_node,
            'mem': self.memory,
            'wt_hours': floor(self.walltime / (60*60)),
            'wt_minutes': floor(self.walltime / 60),
            'wt_seconds': self.walltime % 60,
            'job_name': self.job_name
        }

    def get_slurm_script_file_path(self):
        """
        Returns the full path to the slurm script

        :return: The full path to the slurm script
        """
        return os.path.join(self.get_working_directory(), str(self.ui_id) + '.sh')

    def submit(self, job_parameters):
        """
        Used to submit a job on the cluster

        :param job_parameters: The job parameters for this job
        :return: An integer identifier for the submitted job
        """
        # Read the slurm template
        template = open(self.slurm_template).read()

        # Render the template
        template = template % self.generate_template_dict()

        # Get the output path for this job
        working_directory = self.get_working_directory()

        # Make sure that the directory is deleted if it already exists
        try:
            shutil.rmtree(working_directory)
        except:
            pass

        # Make sure the working directory is recreated
        os.makedirs(working_directory, 0o770, True)

        # Get the path to the slurm script
        slurm_script = self.get_slurm_script_file_path()

        # Save the slurm script
        with open(slurm_script, 'w') as f:
            f.write(template)

        return self.ui_id

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