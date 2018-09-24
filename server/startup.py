import asyncio
import os
import socket
import urllib.parse
from contextlib import closing
from threading import Thread
from time import sleep

import websockets

from .server import poll_cluster_connections, handle_client, domain_socket_client_connected

# todo: document and move to settings
# The port to listen on for websocket connections
HPC_WEBSOCKET_PORT = 8001

# The path to the unix socket for
HPC_IPC_UNIX_SOCKET = "/tmp/job_controller.sock"


def check_if_socket_open(host, port):
    """
    Checks if a socket is open or not on the given host

    :param host: The IP address (as a string) of the host to check
    :param port: The port number to check
    :return: True if the port is open, False if the port is closed
    """
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        return sock.connect_ex((host, port)) == 0


async def new_client(websocket, path):
    """
    Called when a new client connects to the websocket

    :param websocket: The websocket object to communicate with the client
    :param path: The url path (including any url parameters)
    :return: Nothing
    """
    # First split the url parameters out of the path
    parsed = urllib.parse.urlparse(path)
    path = parsed.path
    params = urllib.parse.parse_qs(parsed.query)

    # Check that a token was provided
    if 'token' not in params:
        # No token provided, nothing to do
        return

    # Try to get the websocket token object from the provided token
    try:
        from ..models import WebsocketToken
        token = WebsocketToken.objects.get(token=params['token'][0], used=False)
        
        # Mark the token used
        token.used = True
        token.save()
    except:
        # Token seems to be invalid
        print("Someone tried to reuse token {}".format(params['token'][0]))
        return

    # Verify that the path is correct
    if path not in ['/pipe/', '/file/']:
        # Invalid path provided, nothing to do
        print("Someone tried to connect to an invalid path")
        return

    # Verify that the path is correct for the token type
    if (path == '/pipe/' and token.is_file) or (path == '/file/' and not token.is_file):
        print("Someone tried to connect with the wrong type of token")

    await handle_client(websocket, path, token)


async def domain_socket_server():
    sock = HPC_IPC_UNIX_SOCKET
    
    # Make sure the socket does not already exist
    try:
        os.unlink(sock)    
    except OSError:
        if os.path.exists(sock):
            raise

    # Start the unix domain socket server
    await asyncio.start_unix_server(domain_socket_client_connected, sock)


def websocket_server_thread():
    """
    Thread that runs the websocket server

    :return: Nothing - never returns
    """

    # Create and set the event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Create the websocket server
    start_server = websockets.serve(new_client, 'localhost', HPC_WEBSOCKET_PORT, max_size=2 ** 32, compression=None)

    asyncio.ensure_future(domain_socket_server())

    # Start the websocket server
    loop.run_until_complete(start_server)
    loop.run_forever()


def check_websocket_server():
    """
    Checks that the websocket server is currently running on port 81

    :return: Nothing
    """

    # Check if the websocket server is running
    if not check_if_socket_open("127.0.0.1", HPC_WEBSOCKET_PORT):
        # It's not yet running, so create a thread to run the websocket server
        Thread(target=websocket_server_thread, args=[], daemon=True).start()
        # Wait for the websocket server to start
        while not check_if_socket_open("127.0.0.1", HPC_WEBSOCKET_PORT):
            sleep(0.1)

        # Spawn a new thread to initiate cluster connections
        Thread(target=poll_cluster_connections, args=[], daemon=True).start()


