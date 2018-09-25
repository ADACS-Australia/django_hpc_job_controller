import asyncio
import struct
from asyncio import Queue
from time import sleep

from django.apps import apps

from django_hpc_job_controller.client.core.messaging.message import Message

# The list of currently connected clusters in format
# {Websocket: {'token': WebsocketToken object, 'queue': Queue object}}
CONNECTION_MAP = {}


def send_message_writer(message, sock, raw=False):
    """
    Sends a standard Message object directly to the specified socket
    :param raw: If message is a raw byte array or a Message object
    :param message: The Message object to send (or raw bytes)
    :param sock: The socket to send the message to
    :return: nothing
    """
    # Send the size of the message first
    sock.write(struct.pack('i', message.size() if not raw else len(message)))

    # Send the payload of the message
    sock.write(message.to_bytes() if not raw else message)


async def recv_message_reader(sock):
    """
    Receives a standard Message object directly from the specified socket
    :param sock: The socket to receive the message from
    :return: The received Message object
    """

    # Wait for the response and get the size of the returned message
    size = struct.unpack('I', await sock.read(4))[0]

    # Create a buffer to receive the data
    data = b''
    # While we have not read all the data receive the next chunk
    while len(data) < size:
        data += await sock.read(size - len(data))

    # Create and return the message from the data
    return Message(data=data)


def poll_cluster_connections():
    """
    Manages polling connections

    :return: Never returns
    """
    # Wait for apps to be ready
    while not apps.apps_ready:
        sleep(0.1)

    # Delete all existing websocket tokens
    from django_hpc_job_controller.models import WebsocketToken
    WebsocketToken.objects.all().delete()

    # Loop forever
    while True:
        # Iterate over all clusters
        from django_hpc_job_controller.models import HpcCluster
        for cluster in HpcCluster.objects.all():
            # Ask the cluster to connect
            cluster.try_connect()

        # Wait for 60 seconds before retrying
        sleep(60)


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
    msg = await recv_message_reader(reader)

    # Confirm that the message is SET_FILE_CONNECTION_FILE_DETAILS
    if not msg.pop_uint() == Message.SET_FILE_CONNECTION_FILE_DETAILS:
        raise Exception("Didn't get expected message reading from UDS, expected SET_FILE_CONNECTION_FILE_DETAILS")

    # Send the message on to the client
    await sock.send(msg.to_bytes())

    # Wait for the response
    msg = await sock.recv()

    # Send the message to the uds server
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


async def on_receive(sock, token, queue, message):
    print("Got message from " + token.cluster.host_name + " with token " + str(token.token) + ": " + str(message))


async def send_handler(sock, token, queue):
    """
    Handles sending messages from the queue to the client

    :param sock: The websocket for the client
    :param token: The token the client is connected with
    :param queue: The asyncio queue containing messages to send
    :return: Nothing
    """

    while True:
        message = await queue.get()
        print("Sending message", message)
        await sock.send(message)


async def recv_handler(sock, token, queue):
    while True:
        message = await sock.recv()
        print("Got message", message)
        await on_receive(sock, token, queue, message)


async def handle_client(sock, path, token):
    """
    Handles a new connected client

    :param sock: The websocket to communicate with the client
    :param path: The websocket path
    :param token: The WebsocketToken object this client is using
    :return: Nothing
    """

    # If this is a file connection, we need to connect to the unix domain socket expecting this connection
    if path == '/file/':
        await file_handler(sock, token)
        return

    try:
        # Create a queue to use for this client
        queue = Queue()

        # Add the client to the connection map
        CONNECTION_MAP[sock] = {'token': token, 'queue': queue}

        # Create the consumer and producer tasks
        consumer_task = asyncio.ensure_future(
            recv_handler(sock, token, queue))
        producer_task = asyncio.ensure_future(
            send_handler(sock, token, queue))

        # Wait for one of the tasks to finish
        done, pending = await asyncio.wait(
            [consumer_task, producer_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Kill the remaining tasks
        for task in pending:
            task.cancel()

    finally:
        # The client died, or disconnected, get the cluster
        cluster = token.cluster
        # Remove the client from the connection map so the cluster appears offline
        del CONNECTION_MAP[sock]
        # Try to force reconnect the cluster if this was not a file connection
        cluster.try_connect(True)


def get_socket_from_token(token):
    """
    Returns the socket and token/queue dict for the specified token

    :param token: The token of the connection to check
    :return: The websocket and dict if found or None
    """
    # Iterate over the connections
    for sock in CONNECTION_MAP:
        # Check if this socket is responsible for the requested token
        if str(CONNECTION_MAP[sock]['token'].token) == token:
            return sock, CONNECTION_MAP[sock]

    # Not found
    return None, None


def get_socket_from_cluster_id(cluster_id):
    """
    Returns the socket and token/queue dict for the specified cluster id

    :param cluster_id: The if of the cluster to check
    :return: The websocket and dict if found or None
    """
    # Iterate over the connections
    for sock in CONNECTION_MAP:
        # Check if this socket is responsible for the requested cluster (Ignoring file connections)
        if not CONNECTION_MAP[sock]['token'].is_file and CONNECTION_MAP[sock]['token'].cluster.id == cluster_id:
            # Found it
            return sock, CONNECTION_MAP[sock]

    return None, None


async def domain_socket_client_connected(reader, writer):
    """
    Handles a new unix domain connection

    :param reader: The socket reader
    :param writer: The socket writer
    :return: Nothing
    """
    # Read the message from the socket
    msg = await recv_message_reader(reader)

    # Read the message id
    msg_id = msg.pop_uint()

    # Create a return message
    result = Message(Message.RESULT_OK)

    # Handle the message
    if msg_id == Message.IS_CLUSTER_ONLINE:
        # Get the id of the cluster to check
        cluster_id = msg.pop_uint()

        # Get the socket from the cluster id
        s, m = get_socket_from_cluster_id(cluster_id)

        # Check that the cluster was found
        if m:
            # Push the id of the token for this connection
            result.push_uint(m['token'].id)
        else:
            # No, set the cluster id to 0
            result.push_uint(0)

    elif msg_id == Message.TRANSMIT_WEBSOCKET_MESSAGE:
        # Get the socket to send the websocket message to
        s, m = get_socket_from_token(msg.pop_string())

        # Check that the connection was found
        if m:
            # Send the message to the client
            await m['queue'].put(msg.pop_bytes())
        else:
            # Couldn't find the connection - client is not online
            result = Message(Message.RESULT_FAILURE)
            result.push_string("Unable to find any connected client with the specified token")
    else:
        # Fell through without handling the message
        result = Message(Message.RESULT_FAILURE)
        result.push_string("Unknown message id {}".format(msg_id))

    send_message_writer(result, writer)
