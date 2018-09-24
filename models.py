import io
import random
import uuid
from threading import Thread

import paramiko as paramiko
from django.db import models
from django.utils import timezone


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
        Check if this cluster is currently connected

        :return: True if this cluster is connected otherwise False
        """

        from .server.api import is_cluster_online
        return is_cluster_online(self)


class HpcJob(models.Model):
    """
    A Job
    """

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
