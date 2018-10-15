import logging
import os
import shutil
import subprocess
import uuid
from math import floor

from .status import JobStatus
from .scheduler import Scheduler


class Slurm(Scheduler):
    """
    Slurm stub scheduler - this class should be inherited and extended to provide custom business logic
    """

    STATUS = {
        'BOOT_FAIL': 'Job terminated due to launch failure, typically due to a hardware failure (e.g. unable to boot '
                     'the node or block and the job can not be requeued).',
        'CANCELLED': 'Job was explicitly cancelled by the user or system administrator. The job may or may not have '
                     'been initiated.',
        'COMPLETED': 'Job has terminated all processes on all nodes with an exit code of zero.',
        'DEADLINE': 'Job terminated on deadline.',
        'FAILED': 'Job terminated with non-zero exit code or other failure condition.',
        'NODE_FAIL': 'Job terminated due to failure of one or more allocated nodes.',
        'OUT_OF_MEMORY': 'Job experienced out of memory error.',
        'PENDING': 'Job is awaiting resource allocation.',
        'PREEMPTED': 'Job terminated due to preemption.',
        'RUNNING': 'Job currently has an allocation.',
        'REQUEUED': 'Job was requeued.',
        'RESIZING': 'Job is about to change size.',
        'REVOKED': 'Sibling was removed from cluster due to other cluster starting the job.',
        'SUSPENDED': 'Job has an allocation, but execution has been suspended and CPUs have been released for '
                     'other jobs.',
        'TIMEOUT': 'Job terminated upon reaching its time limit.'
    }

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
            'wt_hours': floor(self.walltime / (60 * 60)),
            'wt_minutes': floor(self.walltime / 60) % 60,
            'wt_seconds': self.walltime % 60,
            'job_name': self.job_name,
            'ui_job_id': self.ui_id
        }

    def get_slurm_script_file_path(self):
        """
        Returns the full path to the slurm script

        :return: The full path to the slurm script
        """
        return os.path.join(self.get_working_directory(), str(self.ui_id) + '.sh')

    def _submit(self, job_parameters):
        """
        Used to submit a job on the cluster

        Entry to the submit function. This function is called by the job controller. Override this with any before/after
        job submission logic specific to the scheduler. Call submit from this function

        :param job_parameters: The job parameters for this job
        :return: An integer identifier for the submitted job
        """
        # Get the output path for this job
        working_directory = self.get_working_directory()

        # Make sure that the directory is deleted if it already exists
        try:
            shutil.rmtree(working_directory)
        except:
            pass

        # Make sure the working directory is recreated
        os.makedirs(working_directory, 0o770, True)

        # Actually submit the job
        return self.submit(job_parameters)

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

        # Get the path to the slurm script
        slurm_script = self.get_slurm_script_file_path()

        # Save the slurm script
        with open(slurm_script, 'w') as f:
            f.write(template)

        # Construct the sbatch command
        command = "cd {} && sbatch {}".format(working_directory, slurm_script)

        # Execute the sbatch command
        stdout = None
        try:
            stdout = subprocess.check_output(command, shell=True)
        except:
            # Record the command and the output
            logging.info("Error: Command `{}` returned `{}`".format(command, stdout))
            return None

        # Record the command and the output
        logging.info("Success: Command `{}` returned `{}`".format(command, stdout))

        # Get the slurm id from the output
        # todo: Handle errors
        try:
            return int(stdout.strip().split()[-1])
        except:
            return None

    def status(self):
        """
        Get the status of a job

        :return: A tuple with JobStatus, additional info as a string. None if no job status could be obtained
        """
        logging.info("Trying to get status of job {}...".format(self.job_id))

        # Construct the command
        command = "sacct -Pn -j {} -o jobid,state%50".format(self.job_id)

        # Execute the sacct command for this job
        stdout = subprocess.check_output(command, shell=True)

        # todo: Handle errors
        # Get the output
        logging.info("Command `{}` returned `{}`".format(command, stdout))

        status = None
        # Iterate over the lines
        for line in stdout.splitlines():
            # Split the line by |
            bits = line.split(b'|')
            # Check that the first bit of the line can be converted to an int (Catches line's containing .batch)
            try:
                if int(bits[0]) == self.job_id:
                    status = bits[1].decode("utf-8")
                    break
            except:
                continue

        logging.info("Got job status {} for job {}".format(status, self.job_id))

        # Check that we got a status for this job
        if not status:
            return None, None

        # Check for general failure
        if status in ['BOOT_FAIL', 'CANCELLED', 'DEADLINE', 'FAILED', 'NODE_FAIL', 'PREEMPTED',
                      'REVOKED']:
            return JobStatus.ERROR, Slurm.STATUS[status]

        # Check for cancelled job
        if status.startswith('CANCELLED'):
            return JobStatus.CANCELLED, status

        # Check for out of memory
        if status == 'OUT_OF_MEMORY':
            return JobStatus.OUT_OF_MEMORY, Slurm.STATUS[status]

        # Check for wall time exceeded
        if status == 'TIMEOUT':
            return JobStatus.WALL_TIME_EXCEEDED, Slurm.STATUS[status]

        # Check for completed successfully
        if status == 'COMPLETED':
            return JobStatus.COMPLETED, Slurm.STATUS[status]
        
        # Check for job currently queued
        if status in ['PENDING', 'REQUEUED', 'RESIZING']:
            return JobStatus.QUEUED, Slurm.STATUS[status]
        
        # Check for job running
        if status in ['RUNNING', 'SUSPENDED']:
            return JobStatus.RUNNING, Slurm.STATUS[status]

        logging.info("Got unknown Slurm job state {} for job {}".format(status, self.job_id))
        return None, None

    def cancel(self):
        """
        Cancel a running job

        :param job_id: The id of the job to cancel
        :return: True if the job was cancelled otherwise False
        """
        logging.info("Trying to terminate job {}...".format(self.job_id))

        # Construct the command
        command = "scancel {}".format(self.job_id)

        # Cancel the job
        stdout = subprocess.check_output(command, shell=True)

        # todo: Handle errors
        # Get the output
        logging.info("Command `{}` returned `{}`".format(command, stdout))

    def delete_data(self):
        """
        Delete all job data

        :return: Nothing
        """
        # Get the output path for this job
        working_directory = self.get_working_directory()

        # Make sure that the directory is deleted if it exists
        try:
            shutil.rmtree(working_directory)
        except:
            pass