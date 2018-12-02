import logging
import os
import pickle
import shutil
import stat
import subprocess

from .status import JobStatus
from .scheduler import Scheduler

SUBMISSION_TEMPLATE = """\
#!/bin/bash

cd %(working_directory)s

bash -e %(script_name)s >%(working_directory)s/log.out 2>%(working_directory)s/log.err

echo $? > %(working_directory)s/exit_code
"""


class Local(Scheduler):
    # Set the name of the local database file
    DATABASE_FILE = 'local.pickle'

    def __init__(self, settings, ui_id, job_id):
        """
        Initialises the job scheduler

        :param ui_id: The UI ID of the job
        :param job_id: The scheduler job id
        """
        # Call the super constructor
        super().__init__(settings, ui_id, job_id)

        # Set the local template
        self.local_template = 'settings/local.sh.template'

        # Create a pickle to use as a database
        if not os.path.exists(Local.DATABASE_FILE):
            # Create the database template
            db = {
                'count': 1
            }
            # Save the database
            self.write_db(db)

    def read_db(self):
        """
        Reads the pickel database and returns it

        :return: The unpickled database
        """
        with open(Local.DATABASE_FILE, 'rb') as db:
            # unpickle and return the database
            return pickle.load(db)

    def write_db(self, db):
        """
        Writes the database as a pickle

        :return: Nothing
        """
        with open(Local.DATABASE_FILE, 'wb') as f:
            # Pickle and write the database
            pickle.dump(db, f)

    def generate_template_dict(self):
        """
        Generates a dictionary to pass to the string formatter for the local template.

        Should be overridden to add custom entries to the dictionary

        :return: The dictionary with the key/value pairs to add to render the local template
        """

        return {
            'ui_job_id': self.ui_id
        }

    def get_local_script_file_path(self):
        """
        Returns the full path to the local run script

        :return: The full path to the local run script
        """
        return os.path.join(self.get_working_directory(), str(self.ui_id) + '.sh')

    def get_local_execution_script_file_path(self):
        """
        Returns the full path to the execution script

        :return: The full path to the execution script
        """
        return os.path.join(self.get_working_directory(), 'run_local.sh')
    
    def get_pid_path(self):
        """
        Returns the full path to the process id file

        :return: The full path to the process id file
        """
        return os.path.join(self.get_working_directory(), 'pid')

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

        # Generate the actual job script to run
        self.submit(job_parameters)

        # Prepare the local execution script
        script = SUBMISSION_TEMPLATE % {
            'working_directory': self.get_working_directory(),
            'script_name': self.get_local_script_file_path()
        }

        # Write the local execution script
        with open(self.get_local_execution_script_file_path(), 'w') as f:
            f.write(script)

        # Read the local template
        template = open(self.local_template).read()

        # Render the template
        template = template % self.generate_template_dict()

        # Get the path to the local script
        local_script = self.get_local_script_file_path()

        # Save the local script
        with open(local_script, 'w') as f:
            f.write(template)

        # Make both generated files executable
        os.chmod(self.get_local_execution_script_file_path(), stat.S_IRUSR | stat.S_IXUSR)
        os.chmod(local_script, stat.S_IRUSR | stat.S_IXUSR)

        # Execute the job in the background
        os.system("set -m; exec nohup {} & echo $! > {}".format(self.get_local_execution_script_file_path(), self.get_pid_path()))

        # Generate a new id for this job
        # Read the database
        db = self.read_db()
        # Get the current id
        local_id = db['count']
        # Increment the id counter
        db['count'] += 1
        # Write the database again
        self.write_db(db)

        # Return the id
        return local_id

    def submit(self, job_parameters):
        """
        Used to submit a job on the cluster

        :param job_parameters: The job parameters for this job
        :return: Nothing, the integer identifier for the job is returned from _submit for Local
        """
        pass

    def check_pid(self, pid):
        """
        Check For the existence of a unix pid.

        :param pid: The process id of the process to check
        :return: True if the process is running otherwise False
        """
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        else:
            return True

    def get_process_id(self):
        """
        Returns the process id of the job

        :return: The process id of the job
        """
        # Get the process id of the job
        return int(open(self.get_pid_path(), 'r').read().strip())

    def status(self):
        """
        Get the status of a job

        :return: A tuple with JobStatus, additional info as a string
        """
        # Get the path to the exit code file
        exit_code_path = os.path.join(self.get_working_directory(), 'exit_code')

        # Check if the process for the job is still running
        if self.check_pid(self.get_process_id()) and not os.path.exists(exit_code_path):
            # No, the job is still running
            return JobStatus.RUNNING, "Job is running"

        # Read the exit code from the exit code file
        try:
            exit_code = int(open(exit_code_path, 'r').read().strip())
            logging.info(str(exit_code))
            # Check if the job executed successfully
            if exit_code == 0:
                # Yes, return success
                return JobStatus.COMPLETED, "Job successfully completed"
            elif exit_code == 2:
                # No, job was cancelled
                return JobStatus.CANCELLED, "Job was cancelled"
            else:
                # No, return error
                return JobStatus.ERROR, "Job failed with exit code {}".format(exit_code)
        except:
            # Some other problem happened
            return JobStatus.ERROR, "Job did not emit an exit code, probably it was killed externally"

    def cancel(self):
        """
        Cancel a running job
        """
        logging.info("Trying to terminate job {}...".format(self.job_id))

        # Construct the command
        command = 'kill -9 -- -$(ps -o pgid= {} | grep -o [0-9]*)'.format(self.get_process_id())

        # Cancel the job
        stdout = subprocess.check_output(command, shell=True)

        # todo: Handle errors
        # Get the output
        logging.info("Command `{}` returned `{}`".format(command, stdout))

        # Get the path to the exit code file
        exit_code_path = os.path.join(self.get_working_directory(), 'exit_code')

        # Mark the job as cancelled
        open(exit_code_path, 'w').write("2")

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