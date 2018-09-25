import socket
import struct

from django_hpc_job_controller.client.core.messaging.message import Message
from django_hpc_job_controller.server.settings import HPC_IPC_UNIX_SOCKET


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




