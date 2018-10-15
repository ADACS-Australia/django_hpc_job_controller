import asyncio
import importlib
import logging
import os
import pickle
import sys
import traceback
from asyncio import sleep
from threading import Thread

import websockets

from scheduler.status import JobStatus
from .db import get_job_by_ui_id, update_job, delete_job, get_all_jobs
from .file_controller import create_file_connection
from .messaging.message import Message


class JobController:

    def __init__(self, argv, settings):
        self.argv = argv
        self.settings = settings
        self.sock = None
        self.queue = asyncio.Queue()
        self.scheduler_klass = self.get_scheduler_instance()

    def get_scheduler_instance(self):
        """
        Returns the class specified by the HPC_SCHEDULER_CLASS setting

        :return: The Class identified by HPC_SCHEDULER_CLASS
        """
        # Split the class path by full stops
        class_bits = self.settings.HPC_SCHEDULER_CLASS.split('.')

        # Import and return the class
        return getattr(importlib.import_module('.'.join(class_bits[:-1])), class_bits[-1])

    async def check_job_status(self, job, force_notification=False):
        """
        Checks a job to see what it's current status is on the cluster. If the job state has changed since we last
        checked, then update the server. If force_notification is True, it will update the server even if the job
        status hasn't changed

        :param job: The job to check
        :param force_notification: If we should notify the server of the job status even if it hasn't changed
        :return: Nothing
        """
        # Check that this job actually has a local id to check
        if not job['job_id']:
            return

        # Get a scheduler for this job
        scheduler = self.scheduler_klass(self.settings, job['ui_id'], job['job_id'])

        # Get the status of the job
        status, info = scheduler.status()

        # Check if the status has changed or not
        if job['status'] != status or force_notification:
            # Send the status to the server to assure receipt in case the job is to be deleted from our database
            result = Message(Message.UPDATE_JOB)
            result.push_uint(job['ui_id'])
            result.push_uint(status)
            result.push_string(info)
            await self.sock.send(result.to_bytes())

            # Check if we should delete the job from the database
            if status > JobStatus.RUNNING:
                # Yes, the job is no longer running, remove it from the database
                await delete_job(job)
            else:
                # Update and save the job status
                job['status'] = status
                await update_job(job)

    async def check_job_status_thread(self):
        """
        Thread entry point for checking the current status of running jobs

        :return: Nothing
        """
        while True:
            try:
                jobs = await get_all_jobs()
                logging.info("Jobs {}".format(str(jobs)))
                for job in jobs:
                    await self.check_job_status(job)
            except Exception as Exp:
                # An exception occurred, log the exception to the log
                logging.error("Error in check job status")
                logging.error(type(Exp))
                logging.error(Exp.args)
                logging.error(Exp)

                # Also log the stack trace
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logging.error(''.join('!! ' + line for line in lines))

            await sleep(60)

    async def handle_message(self, message, identifier=None):
        """
        Handles an incoming websocket message

        :param identifier: A transaction identifier for an assured message
        :param message: The message to handle
        :return: Nothing
        """
        # Convert the raw message to a Message object
        msg = Message(data=message)

        # Get the message id
        msg_id = msg.pop_uint()

        # Handle the message

        if msg_id == Message.HEARTBEAT_PING:
            result = Message(Message.HEARTBEAT_PONG)
            await self.send_assured_message(result, identifier)

        elif msg_id == Message.INITIATE_FILE_CONNECTION:
            # Create a new thread to handle a file connection
            Thread(target=create_file_connection, args=[msg.pop_string(), self.settings], daemon=True).start()

        elif msg_id == Message.SUBMIT_JOB:
            # Get the ui id of the job
            ui_id = msg.pop_uint()
            # Get the parameters for the job
            job_params = pickle.loads(msg.pop_bytes())

            logging.info("Attempting to submit new job with UI ID: {}".format(ui_id))

            # Check if this job has already been submitted by us
            job = await get_job_by_ui_id(ui_id)

            if job and job['job_id']:
                logging.info("Job with ui id {} has already been submitted, checking status...".format(ui_id))
                # If so, check the state of the job and notify the server of it's current state
                await self.check_job_status(job, True)
            else:
                # If not, submit the job and record that we have submitted the job
                logging.info("Submitting new job with ui id {}".format(ui_id))

                # Instantiate the scheduler
                scheduler = self.scheduler_klass(self.settings, ui_id, None)

                # Create a new job object and save it
                job = {'ui_id': ui_id, 'job_id': None, 'status': JobStatus.SUBMITTING}
                await update_job(job)

                # Submit the job
                job_id = scheduler._submit(job_params)

                # Check if there was an issue with the job
                if not job_id:
                    logging.info("Job with ui id {} could not be submitted"
                                 .format(job['ui_id'], job['job_id']))
                    await delete_job(job)
                    # Notify the server that the job is failed
                    result = Message(Message.UPDATE_JOB)
                    result.push_uint(ui_id)
                    result.push_uint(JobStatus.ERROR)
                    result.push_string("Unable to submit job. Please check the logs as to why.")
                    # Send the result
                    await self.sock.send(result.to_bytes())
                else:
                    # Update and save the job
                    job['job_id'] = job_id
                    await update_job(job)

                    logging.info("Successfully submitted job with ui id {}, got scheduler id {}"
                                 .format(job['ui_id'], job['job_id']))

                    # Notify the server that the job is successfully submitted
                    result = Message(Message.SUBMIT_JOB)
                    result.push_uint(ui_id)
                    # Send the result
                    await self.sock.send(result.to_bytes())

                    # Wait for a few seconds
                    await sleep(5)

                    # Check the job status in case it was queued immediately
                    await self.check_job_status(job, True)

        elif msg_id == Message.TRANSMIT_ASSURED_RESPONSE_WEBSOCKET_MESSAGE:
            # Read the identifier
            response_identifier = msg.pop_string()
            # Handle the message
            await self.handle_message(msg.pop_bytes(), response_identifier)

        elif msg_id == Message.GET_FILE_TREE:
            # Get the UI ID of the job to get the files for
            ui_id = msg.pop_uint()
            # Get the path to find the get the files for
            path = msg.pop_string()
            # Check if we are getting a recursive tree
            is_recursive = msg.pop_bool()

            logging.info("Getting file list for job with ui id {}, path {}, recursive {}, identifier {}".format(
                ui_id, path, is_recursive, identifier))

            # Instantiate the scheduler
            scheduler = self.scheduler_klass(self.settings, ui_id, None)

            # Get the actual path on the filesystem for the files requested
            root_path = os.path.join(scheduler.get_working_directory(), path if len(path) and path[0] != os.sep else '')

            # Get the list of files requested
            file_list = []
            if is_recursive:
                # This is a recursive searh
                for root, dirnames, filenames in os.walk(root_path):
                    # Iterate over the directories
                    for item in dirnames:
                        # Construct the real path to this directory
                        real_file_name = os.path.join(root, item)
                        # Add the file entry
                        file_list.append({
                            # Remove the leading working directory
                            'path': real_file_name[len(scheduler.get_working_directory()):],
                            'is_dir': True,
                            'size': os.path.getsize(real_file_name)
                        })

                    for item in filenames:
                        # Construct the real path to this file
                        real_file_name = os.path.join(root, item)
                        # Add the file entry
                        file_list.append({
                            # Remove the leading working directory
                            'path': real_file_name[len(scheduler.get_working_directory()):],
                            'is_dir': False,
                            'size': os.path.getsize(real_file_name)
                        })
            else:
                # Not a recursive search
                for item in os.listdir(root_path):
                    # Construct the real path to this file/directory
                    real_file_name = os.path.join(root_path, item)
                    # Add the file entry
                    file_list.append({
                        # Remove the leading slash
                        'path': real_file_name[len(scheduler.get_working_directory()):],
                        'is_dir': os.path.isdir(real_file_name),
                        'size': os.path.getsize(real_file_name)
                    })

            # Build the return message
            result = Message(Message.GET_FILE_TREE)
            result.push_uint(len(file_list))
            for f in file_list:
                result.push_string(f['path'])
                result.push_bool(f['is_dir'])
                result.push_ulong(f['size'])

            # Send the message back to the server
            await self.send_assured_message(result, identifier)

        elif msg_id == Message.CANCEL_JOB:
            # Get the ui id of the job
            ui_id = msg.pop_uint()

            # Get the job from the database
            job = await get_job_by_ui_id(ui_id)

            # Verify that this job exists
            if not job:
                logging.info("Attempt to cancel a job with UI ID {} That does not exist in the database.".format(ui_id))
                return

            # Get the scheduler for this job
            scheduler = self.scheduler_klass(self.settings, job['ui_id'], job['job_id'])

            # Ask the scheduler to cancel the job
            scheduler.cancel()

            # wait a few seconds then check the status of the job again
            await sleep(5)

            # Check the job status in case it was cancelled immediately
            await self.check_job_status(job, True)

        elif msg_id == Message.DELETE_JOB:
            # Get the ui id of the job
            ui_id = msg.pop_uint()

            # Get the scheduler for this job
            scheduler = self.scheduler_klass(self.settings, ui_id, None)

            try:
                # Ask the scheduler to delete the job data
                scheduler.delete_data()

                # Mark the job deleted
                result = Message(Message.UPDATE_JOB)
                result.push_uint(ui_id)
                result.push_uint(JobStatus.DELETED)
                result.push_string("All Job Data successfully deleted.")
                await self.sock.send(result.to_bytes())
            except Exception as Exp:
                # An exception occurred, log the exception to the log
                logging.error("Error while deleting job data")
                logging.error(type(Exp))
                logging.error(Exp.args)
                logging.error(Exp)

                # Also log the stack trace
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logging.error(''.join('!! ' + line for line in lines))

        else:
            logging.info("Got unknown message id {}".format(msg_id))

    async def send_assured_message(self, message, identifier):
        """
        Sends and assured encapsulated message over the websocket back to the server

        :param message: The message to send
        :param identifier: The identifier for this transaction
        :return: Nothing
        """
        # Create the encapsulated message
        encapsulated_msg = Message(Message.TRANSMIT_ASSURED_RESPONSE_WEBSOCKET_MESSAGE)
        encapsulated_msg.push_string(identifier)
        encapsulated_msg.push_bytes(message.to_bytes())

        # Send the message
        await self.queue.put(encapsulated_msg.to_bytes())

    async def send_handler(self):
        """
        Handles sending messages from the queue to the server

        :return: Nothing
        """
        # Automatically handles sending messages added to the queue
        while True:
            # Wait for a message from the queue
            message = await self.queue.get()
            # Send the message
            await self.sock.send(message)

    async def recv_handler(self):
        """
        Handles receiving messages from the client

        :return: Nothing
        """
        # Loop forever
        while True:
            # Wait for a message to arrive on the websocket
            message = await self.sock.recv()
            # Handle the message
            await self.handle_message(message)

    async def run(self):
        """
        Called to create a websocket connection to the server and manage incoming messages
        :return: Nothing
        """

        self.sock = await websockets.connect(
            '{}/pipe/?token={}'.format(self.settings.HPC_WEBSOCKET_SERVER, self.argv[1]))

        # Create the consumer and producer tasks
        consumer_task = asyncio.ensure_future(self.recv_handler())
        producer_task = asyncio.ensure_future(self.send_handler())

        asyncio.ensure_future(self.check_job_status_thread())

        # Wait for one of the tasks to finish
        done, pending = await asyncio.wait(
            [consumer_task, producer_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Kill the remaining tasks
        for task in pending:
            task.cancel()

    def execute(self):
        """
        Main entry point of the Job Controller - called by the daemon once it is initialised
        :return: Nothing
        """

        # Start the websocket connection
        asyncio.get_event_loop().run_until_complete(self.run())
