import asyncio

from django.utils import timezone

from django_hpc_job_controller.client.core.messaging.message import Message
from django_hpc_job_controller.client.scheduler.status import JobStatus
from django_hpc_job_controller.server.utils import get_job_submission_lock, get_job_model_instance


async def handle_message(sock, token, queue, message):
    """
    Handles an incoming message from a non file websocket

    :param sock: The socket that the message was received from
    :param token: The token used for this websocket connection
    :param queue: The queue to send messages on
    :param message: The raw message received
    :return: Nothing
    """
    # Convert the raw message to a Message object
    msg = Message(data=message)

    # Get the message id
    msg_id = msg.pop_uint()

    if msg_id == Message.SUBMIT_JOB:
        # Acquire the job submission lock
        with get_job_submission_lock():
            # Look up the job
            job = get_job_model_instance().objects.get(id=msg.pop_uint(), job_status=JobStatus.SUBMITTING)

            # Mark the job submitted
            job.job_status = JobStatus.SUBMITTED

            # Set the submitted time
            job.job_submitted_time = timezone.now()

            # Save the job
            job.save()

    elif msg_id == Message.UPDATE_JOB:
        # Look up the job we are updating the status of
        job = get_job_model_instance().objects.get(id=msg.pop_uint())

        # Check that the jobs cluster matches the tokens cluster
        if job.cluster != token.cluster:
            print("A different cluster ({} (id: {})) tried to update a job ({} (id: {})) it does not own!".format(
                str(token.cluster), token.cluster.id, str(job), job.id
            ))
            return

        # Set the new status
        job.job_status = msg.pop_uint()

        # Set the extra details if there are any
        job.job_details = (job.job_details or '') + "{}: New status: {}\n{}\n\n".format(
            timezone.now(), job.job_status, msg.pop_string() or 'No detail')

        # Check if we need to update various time stamps
        if job.job_status == JobStatus.QUEUED:
            job.job_queued_time = timezone.now()

        if job.job_status == JobStatus.RUNNING:
            if not job.job_queued_time:
                job.job_queued_time = timezone.now()
            job.job_running_time = timezone.now()

        if job.job_status in [JobStatus.CANCELLED, JobStatus.ERROR, JobStatus.WALL_TIME_EXCEEDED,
                              JobStatus.OUT_OF_MEMORY, JobStatus.COMPLETED]:
            if not job.job_queued_time:
                job.job_queued_time = timezone.now()
            if not job.job_running_time:
                job.job_running_time = timezone.now()
            job.job_finished_time = timezone.now()

        # Save the job
        job.save()

    elif msg_id == Message.TRANSMIT_ASSURED_RESPONSE_WEBSOCKET_MESSAGE:
        # Create the socket
        from django_hpc_job_controller.server.settings import HPC_IPC_UNIX_SOCKET
        reader, writer = await asyncio.open_unix_connection(HPC_IPC_UNIX_SOCKET + "." + msg.pop_string())

        # Send the encapsulated message
        data = msg.pop_bytes()
        from django_hpc_job_controller.server.server import send_message_writer
        send_message_writer(data, writer, True)
