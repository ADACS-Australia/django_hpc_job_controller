import importlib
import os
import pickle
import socket
import struct
import uuid
from datetime import timedelta

from django.utils import timezone
from filelock import FileLock

from django_hpc_job_controller.client.core.messaging.message import Message
from django_hpc_job_controller.client.scheduler.status import JobStatus
from django_hpc_job_controller.server.settings import HPC_IPC_UNIX_SOCKET, HPC_JOB_CLASS


def send_message_socket(message, sock):
    """
    Sends a standard Message object directly to the specified socket

    :param message: The Message object to send
    :param sock: The socket to send the message to
    :return: Nothing
    """
    # Send the size of the message first
    sock.send(struct.pack('i', message.size()))

    # Send the payload of the message
    sock.send(message.to_bytes())


def recv_message_socket(sock):
    """
    Receives a standard Message object directly from the specified socket

    :param sock: The socket to receive the message from
    :return: The received Message object
    """
    # Wait for the response and get the size of the returned message
    size = struct.unpack('I', sock.recv(4))[0]

    # Create a buffer to receive the data
    data = b''
    # While we have not read all the data receive the next chunk
    while len(data) < size:
        data += sock.recv(size - len(data))

    # Create and return the message from the data
    return Message(data=data)


def send_uds_message(message, path=HPC_IPC_UNIX_SOCKET):
    """
    Sends a message over the specified unix domain socket to the websocket server and returns a single reply

    :param path: The domain socket path
    :param message: The message to send
    :return: The returned message
    """
    # Create a unix domain socket
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)

    # Connect to the websocket unix domain socket controller
    try:
        sock.connect(path)
    except:
        raise Exception("Unable to connect to the local websocket controller, is it down?")

    # Send the message to the uds socket
    send_message_socket(message, sock)

    # Retrieve the result from the uds socket
    return recv_message_socket(sock)


def check_uds_result(message):
    """
    Checks a return message from the unix domain socket to check that it succeeded

    :param message: The message to check for success
    :return: Nothing
    """
    # Verify that the message was successful
    if message.pop_uint() is not Message.RESULT_OK:
        raise Exception("UDS Message failed with reason: {}".format(message.pop_string()))


def get_job_submission_lock():
    """
    Returns the job submission lock

    :return: The job submission file lock object
    """
    # Lock the /tmp lock file so that multiple
    return FileLock("/tmp/hpc-job-controller-submission-lock")


def get_job_model_instance():
    """
    Returns the model class specified by the HPC_JOB_CLASS setting

    :return: The Class identified by HPC_JOB_CLASS
    """
    # Split the class path by full stops
    class_bits = HPC_JOB_CLASS.split('.')

    # Import and return the class
    return getattr(importlib.import_module('.'.join(class_bits[:-1])), class_bits[-1])


def check_pending_jobs():
    """
    Checks for any jobs marked pending and attempts to submit them

    :return: Nothing
    """

    # Lock the job submission mutex
    with get_job_submission_lock():
        # Clean up the django connection
        from django.db import connection
        connection.close()

        # First get any jobs that have been in the submitting state for longer than 60 seconds
        old_submitting_jobs = get_job_model_instance().objects.filter(
            job_status=JobStatus.SUBMITTING,
            job_submitting_time__lte=timezone.now() - timedelta(seconds=60)
        )

        # Mark any old submitting jobs back to pending
        for job in old_submitting_jobs:
            job.job_status = JobStatus.PENDING
            job.save()

        # Get any pending jobs
        pending_jobs = get_job_model_instance().objects.filter(job_status=JobStatus.PENDING)

        # Iterate over each job and attempt to submit it
        for job in pending_jobs:
            # Check if a cluster was chosen for this job
            if not job.cluster:
                job.cluster = job.choose_cluster(pickle.loads(job.job_parameters))

            # Check that an appropriate cluster was found and that the job is currently in a pending state
            if job.cluster and job.job_status == JobStatus.PENDING:
                # Get the token for the selected cluster if it's online
                token = job.cluster.is_connected()
                # Check if the cluster for this job is online
                if token:
                    try:
                        # Create a submit message
                        msg = Message(Message.SUBMIT_JOB)
                        msg.push_uint(job.id)
                        msg.push_bytes(job.job_parameters)

                        # Send the message
                        token.send_message(msg)

                        # Mark the job as submitting
                        job.job_status = JobStatus.SUBMITTING
                        job.job_submitting_time = timezone.now()
                        job.save()
                    except:
                        # The job did not successfully submit, reset it's state back to pending
                        job.job_status = JobStatus.PENDING
                        job.save()

        # Get any cancelling jobs
        cancelling_jobs = get_job_model_instance().objects.filter(job_status=JobStatus.CANCELLING)

        # Iterate over each job and attempt to cancel it
        for job in cancelling_jobs:
            # Check that the job is currently in a cancelling state
            if job.job_status == JobStatus.CANCELLING:
                # Get the token for the selected cluster if it's online
                token = job.cluster.is_connected()
                # Check if the cluster for this job is online
                if token:
                    try:
                        # Create a cancel message
                        msg = Message(Message.CANCEL_JOB)
                        msg.push_uint(job.id)

                        # Send the message
                        token.send_message(msg)
                    except:
                        # The job did not successfully cancel
                        pass

        # Get any deleting jobs
        cancelling_jobs = get_job_model_instance().objects.filter(job_status=JobStatus.DELETING)

        # Iterate over each job and attempt to delete it
        for job in cancelling_jobs:
            # Check that the job is currently in a deleting state
            if job.job_status == JobStatus.DELETING:
                # Check that the job has a cluster
                if not job.cluster:
                    # Job can be safely deleted
                    job.job_status = JobStatus.DELETED
                    job.save()
                else:
                    # Get the token for the selected cluster if it's online
                    token = job.cluster.is_connected()
                    # Check if the cluster for this job is online
                    if token:
                        try:
                            # Create a delete message
                            msg = Message(Message.DELETE_JOB)
                            msg.push_uint(job.id)

                            # Send the message
                            token.send_message(msg)
                        except:
                            # The job did not successfully delete
                            pass


def create_uds_server(socket_path, timeout=10):
    """
    Creates a unix domain socket server with the specified identifier and timeout

    :param socket_path: A string uniquely identifying the path to the connection
    :param timeout: The max amount of time to wait for a client
    :return: The socket server
    """
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
    sock.settimeout(timeout)

    # Listen for incoming connections
    sock.listen(1)

    # Return the listening socket
    return sock


def send_message_assure_response(msg, cluster):
    """
    Special function that creates a unix domain socket server, sends a websocket message and then waits for a response
    on the unix domain socket. This is used as a utility function to syncronise async websocket responses

    :param cluster: The HpcCluster to send the message to
    :param msg: The message to send
    :return: The response message
    """
    # Create the uuid identifier
    identifier = str(uuid.uuid4())

    # Create the socket server
    socket_path = HPC_IPC_UNIX_SOCKET + "." + identifier
    sock = create_uds_server(socket_path)

    # Create the encapsulated message
    encapsulated_msg = Message(Message.TRANSMIT_ASSURED_RESPONSE_WEBSOCKET_MESSAGE)
    encapsulated_msg.push_string(identifier)
    encapsulated_msg.push_bytes(msg.to_bytes())

    # Check that the cluster is online
    token = cluster.is_connected()
    if not token:
        raise Exception("Cluster ({}) is not currently online or connected.".format(str(cluster)))

    # Send the message to the cluster
    token.send_message(encapsulated_msg)

    try:
        # Wait for the connection
        connection, client_address = sock.accept()
    except socket.timeout:
        # THe cluster must also be dead

        # Create the close message
        message = Message(Message.CLOSE_WEBSOCKET)
        message.push_string(str(token.token))
        # Send the message
        check_uds_result(send_uds_message(message))

        # Clean up the socket
        sock.close()
        try:
            os.unlink(socket_path)
        except OSError:
            if os.path.exists(socket_path):
                raise

        raise Exception(
            "Attempt to await a response from the cluster didn't respond in a satisfactory length "
            "of time")
    except:
        raise

    # Read the result and verify that the file exists
    msg = recv_message_socket(connection)

    # Clean up the socket
    sock.close()
    try:
        os.unlink(socket_path)
    except OSError:
        if os.path.exists(socket_path):
            raise

    return msg
