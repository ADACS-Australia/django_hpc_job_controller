import io
import json
import logging
import mimetypes
import os
import pickle
import random
import socket
import subprocess
import sys
import traceback
import uuid
from threading import Thread
from time import sleep

import paramiko as paramiko
from django.db import models
from django.http import StreamingHttpResponse
from django.utils import timezone

from django_hpc_job_controller.client.core.messaging.message import Message
from django_hpc_job_controller.client.scheduler.status import JobStatus
from django_hpc_job_controller.server.settings import HPC_FILE_CONNECTION_CHUNK_SIZE, HPC_IPC_UNIX_SOCKET
from django_hpc_job_controller.server.utils import send_uds_message, check_uds_result, \
    send_message_socket, recv_message_socket, check_pending_jobs, \
    create_uds_server, send_message_assure_response

logger = logging.getLogger(__name__)


class WebsocketToken(models.Model):
    """
    A record of current and used websocket tokens

    todo: Need to clean up old tokens, or tokens older than some duration that haven't been used
    """

    # The token
    token = models.UUIDField(default=uuid.uuid4, editable=False)

    # If the token has been used or not
    used = models.BooleanField(default=False)

    # Cluster for this token
    cluster = models.ForeignKey('HpcCluster', models.CASCADE)

    # If this token is for a file connection
    is_file = models.BooleanField(default=False)

    # The timestamp when the token was issued
    timestamp = models.DateTimeField(default=timezone.now)

    def send_message(self, message):
        """
        Sends a message to the websocket associated with this token

        :param message: a Message object to send
        :return: Nothing
        """
        # Create the encapsulating message
        encapsulated = Message(Message.TRANSMIT_WEBSOCKET_MESSAGE)
        encapsulated.push_string(str(self.token))
        encapsulated.push_bytes(message.to_bytes())

        # Send the message
        check_uds_result(send_uds_message(encapsulated))


class HpcCluster(models.Model):
    """
    A record for a HPC Cluster
    """

    # The host name of the cluster, ie ozstar.swin.edu.au
    host_name = models.CharField(max_length=250, help_text="Host name and Client path must be unique together.")

    # THe private SSH key if there is one
    key = models.TextField(blank=True)

    # The username of the user accessing the cluster
    username = models.CharField(max_length=32)

    # Either:
    # A) The password for the SSH private key if it is not blank
    # B) The password of the user if the private key is blank
    password = models.CharField(max_length=1024, blank=True)

    # The absolute path to the client.py file
    client_path = models.CharField(max_length=2048, default='',
                                   help_text="Host name and Client path must be unique together.")

    def __str__(self):
        name = "{}@{}".format(self.username, self.host_name)
        if len(self.key):
            return name + " using key authentication"
        else:
            return name + " using password authentication"

    def get_ssh_connection(self):
        """
        Returns a Paramiko SSH connection to the cluster

        :return: The SSH instance
        """
        # Create an SSH client
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        try:
            # Check for encrypted key
            if self.key and self.password:
                key = io.StringIO(self.key)
                key = paramiko.RSAKey.from_private_key(key, self.password)
                ssh.connect(self.host_name, username=self.username, pkey=key)
                return ssh, ssh.get_transport().open_channel("session")

            # Check for normal key
            if self.key:
                key = io.StringIO(self.key)
                key = paramiko.RSAKey.from_private_key(key)
                ssh.connect(self.host_name, username=self.username, pkey=key)
                return ssh, ssh.get_transport().open_channel("session")

            # Use normal password authentication
            ssh.connect(self.host_name, username=self.username, password=self.password, allow_agent=False,
                        look_for_keys=False)
            return ssh, ssh.get_transport().open_channel("session")
        except Exception as e:
            # An exception occurred, log the exception to the log
            logging.error("Error in get_ssh_connection")
            logging.error(type(e))
            logging.error(e.args)
            logging.error(e)

            # Also log the stack trace
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(''.join('!! ' + line for line in lines))

            # Return None
            return None, None

    def try_connect(self, force=False):
        """
        Checks if this cluster is connected, and if not tries to connect

        :param force: Forces the remote client to be started even if the cluster reports that it is online

        :return: Nothing
        """

        # Check if this cluster is already connected
        if not force and self.is_connected():
            # Yes, nothing to do
            return

        def connection_thread():
            # Generate a new token for the connection
            token = WebsocketToken.objects.create(cluster=self)

            # Check if the host is localhost
            if self.host_name == 'localhost':
                # Use subprocess to start the client locally
                subprocess.check_output(
                    "cd {}; . venv/bin/activate; python client.py {}".format(self.client_path, token.token),
                    shell=True
                )
            else:
                # Try to create the ssh connection
                client, ssh = self.get_ssh_connection()
                if not ssh:
                    # Looks like the server is down, or credentials are invalid
                    return

                # Construct the command
                command = "cd {}; . venv/bin/activate; python client.py {}".format(self.client_path, token.token)

                # Execute the remote command to start the daemon
                ssh.exec_command(command)

                # Wait for the connection to close
                stdout, stderr = b'', b''
                while True:  # monitoring process
                    # Reading from output streams
                    while ssh.recv_ready():
                        stdout += ssh.recv(1000)
                    while ssh.recv_stderr_ready():
                        stderr += ssh.recv_stderr(1000)
                    if ssh.exit_status_ready():  # If completed
                        break
                    sleep(0.1)

                # Close the conneciton
                ssh.close()
                client.close()

                logger.info("SSH command {} returned:".format(command))
                logger.info("Stdout: {}".format(stdout))
                logger.info("Stderr: {}".format(stderr))

        # Spawn a thread to try to connect the client
        Thread(target=connection_thread, args=[], daemon=True).start()

    def is_connected(self):
        """
        Checks if this cluster is currently online

        :return: A WebsocketToken object if the cluster is online otherwise None
        """
        # Create a message to check if the cluster is online
        msg = Message(Message.IS_CLUSTER_ONLINE)
        msg.push_uint(self.id)

        # Send the message
        msg = send_uds_message(msg)
        check_uds_result(msg)

        # Get the id of the WebsocketToken for this cluster
        token_id = msg.pop_uint()

        # If the token id is 0, then the cluster is not currently connected
        if not token_id:
            return None

        # The cluster is online, get the websocket token object and return
        return WebsocketToken.objects.get(cluster=self, id=token_id, is_file=False)

    def fetch_remote_file(self, path, ui_id=None, force_download=True, extra_params=None):
        """
        Fetches a file, path, from a this cluster over a websocket connection, and returns a Streaming HTTP response

        :param force_download: If the returned Streaming HTTP Response should force a download
        :param ui_id: The UI ID of the job to fetch the file for, if this is None files can be fetched from anywhere
        :param path: The path to the file to fetch
        :param extra_params: Any extra details to be sent to the client - this can be used for things like custom
        archive generation (Will be consumed by the client customizations)

        :return: A Django StreamingHTTPResponse
        """
        # Check that the cluster is online
        if extra_params is None:
            extra_params = {}
        token = self.is_connected()
        if not token:
            raise Exception("Cluster ({}) is not currently online or connected.".format(str(self)))

        # Create a token to use for the file websocket
        file_token = WebsocketToken.objects.create(cluster=self, is_file=True)

        # Create the named uds
        socket_path = HPC_IPC_UNIX_SOCKET + "." + str(file_token.token)
        sock = create_uds_server(socket_path)

        # Ask the cluster to raise a new websocket connection for this file
        msg = Message(Message.INITIATE_FILE_CONNECTION)

        # Add the token to the message
        msg.push_string(str(file_token.token))

        # Send the message to the cluster
        token.send_message(msg)

        try:
            # Wait for the connection
            connection, client_address = sock.accept()
        except socket.timeout:
            # Clean up the socket
            sock.close()
            try:
                os.unlink(socket_path)
            except OSError:
                if os.path.exists(socket_path):
                    raise

            # THe websocket must also be dead
            # Create the close message
            message = Message(Message.CLOSE_WEBSOCKET)
            message.push_string(str(token.token))
            # Send the message
            check_uds_result(send_uds_message(message))

            raise Exception(
                "Attempt to create a file connection to the cluster didn't respond in a satisfactory length "
                "of time")
        except:
            raise

        # Set the path to the file we want to fetch
        msg = Message(Message.SET_FILE_CONNECTION_FILE_DETAILS)
        msg.push_uint(ui_id or 0)
        msg.push_string(path)
        msg.push_ulong(HPC_FILE_CONNECTION_CHUNK_SIZE)
        msg.push_string(json.dumps(extra_params))
        send_message_socket(msg, connection)

        # Read the result and verify that the file exists
        msg = recv_message_socket(connection)

        # Check the result
        check_uds_result(msg)

        # Get the file size
        file_size = msg.pop_uint()

        # Now we loop until we have all the chunks from the client
        def stream_generator():
            while True:
                # Read the next chunk
                msg = recv_message_socket(connection)

                # Ignore the message identifier
                msg.pop_uint()

                # Get the raw data for this chunk
                chunk = msg.pop_bytes()

                # Check if this chunk indicates the end of the file
                if not len(chunk):
                    # Clean up the socket
                    sock.close()
                    try:
                        os.unlink(socket_path)
                    except OSError:
                        if os.path.exists(socket_path):
                            raise

                    # Nothig more to do
                    break

                # Return this chunk
                yield chunk

        # Create the streaming http response object
        response = StreamingHttpResponse(stream_generator(), content_type=mimetypes.guess_type(os.path.basename(path)))

        # Set the file size so the browser knows how big the file is
        response['Content-Length'] = file_size

        if force_download:
            # Set the file name of the file the user is downloading
            response['Content-Disposition'] = "attachment; filename=%s" % os.path.basename(path)

        # Finally return the response
        return response


class HpcJob(models.Model):
    """
    A Job
    """

    # The cluster this job is utilising
    cluster = models.ForeignKey(HpcCluster, on_delete=models.CASCADE, null=True, blank=True, default=None)

    # The current status of this job
    job_status = models.IntegerField(null=True, blank=True, default=JobStatus.DRAFT)

    # The time the job was marked pending
    job_pending_time = models.DateTimeField(blank=True, null=True, default=None)

    # The time the job was marked as submitting (Waiting for a response from a client)
    job_submitting_time = models.DateTimeField(blank=True, null=True, default=None)

    # The time the job was submitted
    job_submitted_time = models.DateTimeField(blank=True, null=True, default=None)

    # The time the job was queued
    job_queued_time = models.DateTimeField(blank=True, null=True, default=None)

    # The time the job started running
    job_running_time = models.DateTimeField(blank=True, null=True, default=None)

    # The time the job finished or crashed with an error
    job_finished_time = models.DateTimeField(blank=True, null=True, default=None)

    # Any additional details about the job
    job_details = models.TextField(blank=True, null=True, default=None)

    # Parameters for the job
    job_parameters = models.BinaryField(blank=True, null=True, default=None)

    @property
    def job_status_display(self):
        return JobStatus.display_name(self.job_status)

    def choose_cluster(self, parameters):
        """
        Chooses the most appropriate cluster for the job to run on

        Should be overridden

        :type parameters: Job parameters that may be used to choose a correct cluster for this job

        :return: The cluster chosen to run the job on
        """
        # Get all clusters currently online
        clusters = HpcCluster.objects.all()

        online_clusters = []
        for c in clusters:
            if c.is_connected():
                online_clusters.append(c)

        # Return an online cluster at random
        return random.choice(online_clusters) if len(online_clusters) else None

    def fetch_remote_file_list(self, path="/", recursive=True):
        """
        Retrieves the list of files at the specified relative path and returns it

        :param path: The relative path in the job output directory to fetch the file list for
        :param recursive: If the result should be the recursive list of files
        :return: A recursive dictionary of file information
        """
        msg = Message(Message.GET_FILE_TREE)
        msg.push_uint(self.id)
        msg.push_string(path)
        msg.push_bool(recursive)

        return send_message_assure_response(msg, self.cluster)

    def fetch_remote_file(self, path, force_download=True, extra_params=None):
        """
        Retrieves a file from the remote job working directory specified with the relative directory path

        :param force_download: If the returned Streaming HTTP Response should force a download
        :param path: The relative path to the job working directory of the file to retrieve
        :param extra_params: Any extra details to be sent to the client - this can be used for things like custom
        archive generation (Will be consumed by the client customizations)

        :return: A streaming HTTP response
        """
        if not self.cluster:
            raise Exception("Job does not have a cluster yet!")

        # Fetch the remote file
        return self.cluster.fetch_remote_file(path[1:] if len(path) and path[0] == os.sep else path, self.id,
                                              force_download, extra_params)

    def submit(self, parameters):
        """
        Submits this job to the cluster

        Should not be overridden

        :param parameters: Any python picklable object containing the information to be sent to the client

        :return: Nothing
        """
        # Check that the job is currently a draft
        if self.job_status != JobStatus.DRAFT:
            raise Exception("Attempt to submit a job that is already submitted is invalid")

        # Pickle the parameter data
        self.job_parameters = pickle.dumps(parameters)

        # Choose an appropriate cluster for this job
        self.cluster = self.choose_cluster(parameters)

        # Mark the job as submitted
        self.job_status = JobStatus.PENDING

        # Set the submission time
        self.job_pending_time = timezone.now()

        # Save this job
        self.save()

        # Attempt to submit the job to the client
        check_pending_jobs()

    def cancel(self):
        """
        Cancels the job on the cluster

        :return: Nothing
        """
        # Check that the job is currently in a cancellable state
        if self.job_status not in [JobStatus.PENDING, JobStatus.SUBMITTED, JobStatus.QUEUED, JobStatus.RUNNING]:
            raise Exception("Attempt to cancel a job that is not in a pending, queued, or running state")

        # Check if the job is pending, and mark it cancelled immediately since it was never submitted to the server
        if self.job_status == JobStatus.PENDING:
            self.job_status = JobStatus.CANCELLED
            self.save()
            return

        # Mark the job as cancelling
        self.job_status = JobStatus.CANCELLING

        # Save this job
        self.save()

        # Attempt to submit the job to the client
        check_pending_jobs()

    def delete_job(self):
        """
        Deletes the job and removes the data on the cluster

        :return: Nothing
        """
        # Check that the job is currently in a deletable state
        if self.job_status not in [JobStatus.DRAFT, JobStatus.COMPLETED, JobStatus.ERROR, JobStatus.CANCELLED,
                                   JobStatus.WALL_TIME_EXCEEDED, JobStatus.OUT_OF_MEMORY]:
            raise Exception("Attempt to delete a job that is in a running state")

        # Mark the job as deleting
        self.job_status = JobStatus.DELETING

        # Save this job
        self.save()

        # Attempt to submit the job to the client
        check_pending_jobs()
