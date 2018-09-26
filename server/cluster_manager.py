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

    # Handle the message
    print("Got message from " + token.cluster.host_name + " with token " + str(token.token) + ": " + str(msg.data))

    if msg_id == Message.SUBMIT_JOB:
        # Aquire the job submission lock
        with get_job_submission_lock():
            # Look up the job
            job = get_job_model_instance().objects.get(id=msg.pop_uint(), job_status=JobStatus.SUBMITTING)
            print("Got job", job)
            job.job_status = JobStatus.SUBMITTED
            job.save()