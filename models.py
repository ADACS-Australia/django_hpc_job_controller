import io
import os
import random
import socket
import uuid
from threading import Thread

import paramiko as paramiko
from django.db import models
from django.http import StreamingHttpResponse
from django.utils import timezone

from django_hpc_job_controller.client.core.messaging.message import Message
from django_hpc_job_controller.server.utils import send_uds_message, check_uds_result, \
    send_message_socket, recv_message_socket
from django_hpc_job_controller.server.settings import HPC_FILE_CONNECTION_CHUNK_SIZE


class WebsocketToken(models.Model):
    """
    A record of current and used websocket tokens
    """

    # The token
    token = models.UUIDField(default=uuid.uuid4, editable=False)

    # If the token has been used or not
    used = models.BooleanField(default=False)

    # Cluster for this token
    cluster = models.ForeignKey('HpcCluster', models.CASCADE)

    # If this token is for a file connection
    is_file = models.BooleanField(default=False)

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
    host_name = models.CharField(max_length=250, unique=True)

    # THe private SSH key if there is one
    key = models.TextField(blank=True)

    # The username of the user accessing the cluster
    username = models.CharField(max_length=32)

    # Either:
    # A) The password for the SSH private key if it is not blank
    # B) The password of the user if the private key is blank
    password = models.CharField(max_length=1024, blank=True)

    # The absolute path to the client.py file
    client_path = models.CharField(max_length=2048, default='')

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
                return ssh

            # Check for normal key
            if self.key:
                key = io.StringIO(self.key)
                key = paramiko.RSAKey.from_private_key(key)
                ssh.connect(self.host_name, username=self.username, pkey=key)
                return ssh

            # Use normal password authentication
            ssh.connect(self.host_name, username=self.username, password=self.password)
            return ssh
        except:
            return None

    def try_connect(self, force=False):
        """
        Checks if this cluster is connected, and if not tries to connect
        :return: Nothing
        """

        # Check if this cluster is already connected
        if not force and self.is_connected():
            # Yes, nothing to do
            return

        def connection_thread():
            # Generate a new token for the connection
            token = WebsocketToken.objects.create(cluster=self)

            # Try to create the ssh connection
            ssh = self.get_ssh_connection()
            if not ssh:
                # Looks like the server is down, or credentials are invalid
                return
            # Execute the remote command to start the daemon
            ssh.exec_command(
                "cd {}; . venv/bin/activate; python client.py start {}".format(self.client_path, token.token)
            )

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

    def fetch_remote_file(self, path):
        """
        Fetches a file, path, from a this cluster over a websocket connection, and returns a Streaming HTTP response

        :param path: The path to the file to fetch

        :return: A Django StreamingHTTPResponse
        """
        # Check that the cluster is online
        token = self.is_connected()
        if not token:
            raise Exception("Cluster ({}) is not currently online or connected.".format(str(self)))

        # Create a token to use for the file websocket
        file_token = WebsocketToken.objects.create(cluster=self, is_file=True)

        # Create the unique socket identifier
        from django_hpc_job_controller.server.startup import HPC_IPC_UNIX_SOCKET
        socket_path = HPC_IPC_UNIX_SOCKET + "." + str(file_token.token)

        # Make sure the socket does not already exist
        try:
            os.unlink(socket_path)
        except OSError:
            if os.path.exists(socket_path):
                raise

        # Create a new unix domain socket server to receive the incoming data
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.bind(socket_path)

        # Set the maximum timeout to 10 seconds
        sock.settimeout(10)

        # Listen for incoming connections
        sock.listen(1)

        # Ask the cluster to raise a new websocket connection for this file
        msg = Message(Message.INITIATE_FILE_CONNECTION)

        # Add the token to the message
        msg.push_string(str(file_token.token))

        # Send the message to the cluster
        token.send_message(msg, token)

        try:
            # Wait for the connection
            connection, client_address = sock.accept()
        except socket.timeout:
            raise Exception(
                "Attempt to create a file connection to the cluster didn't respond in a satisfactory length "
                "of time")
        except:
            raise

        # Set the path to the file we want to fetch
        msg = Message(Message.SET_FILE_CONNECTION_FILE_DETAILS)
        msg.push_string(path)
        msg.push_ulong(HPC_FILE_CONNECTION_CHUNK_SIZE)
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
                    print("Done")
                    break

                # Return this chunk
                yield chunk

        # Create the streaming http response object
        response = StreamingHttpResponse(stream_generator())

        # Set the file size so the browser knows how big the file is
        response['Content-Length'] = file_size

        # Set the file name of the file the user is downloading
        response['Content-Disposition'] = "attachment; filename=%s" % os.path.basename(path)

        # Finally return the response
        return response


class HpcJob(models.Model):
    """
    A Job
    """

    # The cluster this job is utilising
    cluster = models.ForeignKey(HpcCluster, on_delete=models.CASCADE, null=True, default=None)

    # The number of CPU cores this job requires
    cpus = models.IntegerField(default=1)

    # The number of Mb of ram this job requires
    ram = models.IntegerField(default=100)

    # The current status of this job
    job_status = models.IntegerField(null=True, default=None)

    # The time the job was submitted
    job_submitted_time = models.DateTimeField(default=timezone.now)

    # The time the job was queued
    job_queued_time = models.DateTimeField(blank=True, null=True, default=None)

    # The time the job finished or crashed with an error
    job_finished_time = models.DateTimeField(blank=True, null=True, default=None)

    def choose_cluster(self):
        """
        Chooses the most appropriate cluster for the job to run on

        Should be overridden

        :return: The cluster chosen to run the job on
        """
        # Return an online cluster at random
        return random.choice(HpcCluster.objects.filter(is_online=True))

    def set_required_cpus(self, cpus):
        """
        Set's the number of cpus the job requires

        :param cpus: The number of cpus
        :return: self
        """
        self.cpus = cpus
        self.save()
        return self

    def set_required_memory(self, memory):
        """
        Set's the amount of memory in Mb the job requires

        :param memory: The amount of memory
        :return: self
        """
        self.ram = memory
        self.save()
        return self

    def submit(self):
        """
        Submits this job to the cluster

        Should not be overridden

        :return: The current status of the job (Either SUBMITTED or QUEUED)
        """
