import asyncio

from django_hpc_job_controller.client.core.messaging.message import Message


async def file_handler(sock, token):
    """
    Handles the proxying of the websocket to Django over the unix domain socket

    :param sock: The websocket connection
    :param token: The websocket token for the websocket connection
    :return: Nothing
    """
    from django_hpc_job_controller.server.startup import HPC_IPC_UNIX_SOCKET
    # Create the socket
    reader, writer = await asyncio.open_unix_connection(HPC_IPC_UNIX_SOCKET + "." + str(token.token))

    # Wait for the server to send us the name of the file to send to the client
    from django_hpc_job_controller.server.server import recv_message_reader
    msg = await recv_message_reader(reader)

    # Confirm that the message is SET_FILE_CONNECTION_FILE_DETAILS
    if not msg.pop_uint() == Message.SET_FILE_CONNECTION_FILE_DETAILS:
        raise Exception("Didn't get expected message reading from UDS, expected SET_FILE_CONNECTION_FILE_DETAILS")

    # Send the message on to the client
    await sock.send(msg.to_bytes())

    # Wait for the response
    msg = await sock.recv()

    # Send the message to the uds server
    from django_hpc_job_controller.server.server import send_message_writer
    send_message_writer(msg, writer, True)

    # Now we loop reading each each chunk and waiting for a zero sized chunk
    while True:
        # Wait for the response
        msg = await sock.recv()

        # Convert the data to a message
        msg = Message(data=msg)

        # Check that the message is SEND_FILE_CHUNK
        if not msg.pop_uint() == Message.SEND_FILE_CHUNK:
            raise Exception("Didn't get expected message reading from File Connection, expected SEND_FILE_CHUNK")

        # Send the message back to the uds server
        send_message_writer(msg, writer)

        try:
            # This exists to catch when the socket closes, since write doesn't raise an exception which is bizarre
            await writer.drain()
        except:
            return

        # Check if this chunk indicates the end of the file stream
        if not len(msg.pop_bytes()):
            break
