import os
import socket
import struct

from django.http import StreamingHttpResponse

from django_hpc_job_controller.client.core.messaging.message import Message
from django_hpc_job_controller.models import WebsocketToken
from django_hpc_job_controller.server.settings import HPC_FILE_CONNECTION_CHUNK_SIZE

IPC_UNIX_SOCKET = "/tmp/job_controller.sock"


def send_message_socket(message, sock):
    """
    Sends a standard Message object directly to the specified socket
    :param message: The Message object to send
    :param sock: The socket to send the message to
    :return: nothing
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


def send_uds_message(message, path=IPC_UNIX_SOCKET):
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


def send_websocket_message(message, connection_token):
    """
    Sends a message to the websocket identified by the provided token

    :param message: a Message object to send
    :param connection_token: The connection token of the currently connected websocket to send the message to
    :return: Nothing
    """
    # Create the encapsulating message
    encapsulated = Message(Message.TRANSMIT_WEBSOCKET_MESSAGE)
    encapsulated.push_string(str(connection_token))
    encapsulated.push_bytes(message.to_bytes())

    # Send the message
    check_uds_result(send_uds_message(encapsulated))


def is_cluster_online(cluster):
    """
    Checks if a cluster is currently online

    :param cluster: The cluster to check
    :return: A WebsocketToken object if the cluster is online otherwise None
    """
    # Create a message to check if the cluster is online
    msg = Message(Message.IS_CLUSTER_ONLINE)
    msg.push_uint(cluster.id)

    # Send the message
    msg = send_uds_message(msg)
    check_uds_result(msg)

    # Get the id of the WebsocketToken for this cluster
    token_id = msg.pop_uint()

    # If the token id is 0, then the cluster is not currently connected
    if not token_id:
        return None

    # The cluster is online, get the websocket token object and return
    return WebsocketToken.objects.get(cluster=cluster, id=token_id, is_file=False)


def fetch_file_from_cluster(cluster, path):
    """
    Fetches a file, path, from a cluster over a websocket connection, and returns a Streaming HTTP response

    :param cluster: The HpcCluster to fetch the file from
    :param path: The path to the file to fetch

    :return: A Django StreamingHTTPResponse
    """
    # Check that the cluster is online
    token = is_cluster_online(cluster)
    if not token:
        raise Exception("Cluster ({}) is not currently online or connected.".format(str(cluster)))

    # Create a token to use for the file websocket
    file_token = WebsocketToken.objects.create(cluster=cluster, is_file=True)

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
    send_websocket_message(msg, token.token)

    try:
        # Wait for the connection
        connection, client_address = sock.accept()
    except socket.timeout:
        raise Exception("Attempt to create a file connection to the cluster didn't respond in a satisfactory length "
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
