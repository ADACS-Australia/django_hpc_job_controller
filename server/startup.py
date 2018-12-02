import asyncio
import logging
import os
import socket
import sys
import traceback
import urllib.parse
from contextlib import closing
from datetime import timedelta
from threading import Thread
from time import sleep

import websockets
from django.utils import timezone

from django_hpc_job_controller.server.settings import HPC_WEBSOCKET_PORT, HPC_IPC_UNIX_SOCKET
from .server import poll_cluster_connections, handle_client, domain_socket_client_connected, heartbeat_thread

# Get the logger
logger = logging.getLogger(__name__)


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

    # Clean up the django connection
    from django.db import connection
    connection.close()

    # Try to get the websocket token object from the provided token
    try:
        from ..models import WebsocketToken
        token = WebsocketToken.objects.get(token=params['token'][0])

        # Check that the token is not older than a minute
        if token.timestamp + timedelta(seconds=60) < timezone.now():
            # Token seems to be invalid
            logger.info("Someone tried to use an expired token {}".format(params['token'][0]))
            # Close the socket
            await websocket.close()
            # Nothing else to do
            return

        # Check if the token has already been used
        if token.used:
            # Token seems to be invalid
            logger.info("Someone tried to reuse token {}".format(params['token'][0]))
            # Close the socket
            await websocket.close()
            # Nothing else to do
            return

        # Mark the token used
        token.used = True
        token.save()
    except Exception as Exp:
        # An exception occurred, log the exception to the log
        logger.error("Error while handling a new client")
        logger.error(type(Exp))
        logger.error(Exp.args)
        logger.error(Exp)

        # Also log the stack trace
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.error(''.join('!! ' + line for line in lines))

    # Verify that the path is correct
    if path not in ['/pipe/', '/file/']:
        # Invalid path provided, nothing to do
        logger.info("Someone tried to connect to an invalid path")
        return

    # Verify that the path is correct for the token type
    if (path == '/pipe/' and token.is_file) or (path == '/file/' and not token.is_file):
        logger.info("Someone tried to connect with the wrong type of token")

    logger.info("Remote cluster {} connected with token {} on path {}".format(
        str(token.cluster), str(token.token), path)
    )

    await handle_client(websocket, path, token)


async def domain_socket_server():
    sock = HPC_IPC_UNIX_SOCKET

    try:
        # Make sure the socket does not already exist
        try:
            os.unlink(sock)
        except OSError:
            if os.path.exists(sock):
                raise

        # Start the unix domain socket server
        await asyncio.start_unix_server(domain_socket_client_connected, sock)
    except Exception as e:
        # An exception occurred, log the exception to the log
        logger.error("Error in domain_socket_server")
        logger.error(type(e))
        logger.error(e.args)
        logger.error(e)

        # Also log the stack trace
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.error(''.join('!! ' + line for line in lines))


def websocket_server_thread():
    """
    Thread that runs the websocket server

    :return: Nothing - never returns
    """

    # In case the websocket server dies
    while True:
        try:
            # Create and set the event loop for this thread
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            # Create the websocket server
            start_server = websockets.serve(new_client, '0.0.0.0', HPC_WEBSOCKET_PORT, max_size=2 ** 32, compression=None)

            asyncio.ensure_future(domain_socket_server())

            # Start the websocket server
            loop.run_until_complete(start_server)
            loop.run_forever()
        except Exception as e:
            # An exception occurred, log the exception to the log
            logger.error("Error in websocket_server_thread")
            logger.error(type(e))
            logger.error(e.args)
            logger.error(e)

            # Also log the stack trace
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error(''.join('!! ' + line for line in lines))


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

        # Spawn a new thread to verify that clients are responsive
        Thread(target=heartbeat_thread, args=[], daemon=True).start()
