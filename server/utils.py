import importlib
import pickle
import socket
import struct
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

                        # Mark the job as submitted
                        job.job_status = JobStatus.SUBMITTING
                        job.job_submitting_time = timezone.now()
                        job.save()
                    except:
                        # The job did not successfully submit, reset it's state back to pending
                        job.job_status = JobStatus.PENDING
                        job.save()
