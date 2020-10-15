import asyncio
import importlib
import json
import logging
import os
import sys
import traceback

import websockets

from .messaging.message import Message


class FileController:
    """
    Manages creating a file websocket connection and sending data over the websocket to the server
    """

    def __init__(self, token, settings):
        self.token = token
        self.settings = settings
        self.file_path = None
        self.file = None
        self.offset = 0
        self.file_size = 0
        self.file_chunk_size = 0
        self.ui_id = None
        self.extra_params = None
        self.ignore_prefix = False
        self.scheduler_klass = self.get_scheduler_instance()

    def get_scheduler_instance(self):
        """
        Returns the class specified by the HPC_SCHEDULER_CLASS setting

        :return: The Class identified by HPC_SCHEDULER_CLASS
        """
        # Split the class path by full stops
        class_bits = self.settings.HPC_SCHEDULER_CLASS.split('.')

        # Import and return the class
        return getattr(importlib.import_module('.'.join(class_bits[:-1])), class_bits[-1])

    async def run(self):
        """
        Called to create a file websocket connection to the server and manage incoming messages

        :return: Nothing
        """
        async with websockets.connect('{}/file/?token={}'.format(self.settings.HPC_WEBSOCKET_SERVER, self.token),
                                      max_size=2 ** 32) as sock:
            logging.info("File controller connected ok with token {}".format(self.token))

            try:
                await self.send_file(sock)
            except Exception as Exp:
                # An exception occurred, log the exception to the log
                logging.error("Error in file transfer")
                logging.error(type(Exp))
                logging.error(Exp.args)
                logging.error(Exp)

                # Also log the stack trace
                exc_type, exc_value, exc_traceback = sys.exc_info()
                lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logging.error(''.join('!! ' + line for line in lines))

    async def send_file(self, sock):
        async for msg in sock:
            # Convert the data to a message
            msg = Message(data=msg)

            # Read the message id
            msg_id = msg.pop_uint()

            # Handle the message
            if msg_id == Message.SET_FILE_CONNECTION_FILE_DETAILS:
                try:
                    # Read the file name from the message
                    self.ui_id = msg.pop_uint() or None
                    self.file_path = msg.pop_string()
                    self.file_chunk_size = msg.pop_ulong()
                    self.extra_params = json.loads(msg.pop_string())

                    # Get an instance of the scheduler
                    scheduler = self.scheduler_klass(self.settings, self.ui_id, None)

                    # Check if there is a file send customisation and call it
                    try:
                        self.file_path, self.ignore_prefix = scheduler.file_send_start(self.file_path, self.extra_params)
                    except AttributeError:
                        pass

                    # Check if we need to construct an absolute file path from a relative path
                    is_valid = True
                    if self.ui_id and not self.ignore_prefix:
                        self.file_path = os.path.join(scheduler.get_working_directory(), self.file_path)
                        self.file_path = os.path.abspath(self.file_path)
                        # Fetching job files must only fetch files within the job directory
                        if not self.file_path.startswith(scheduler.get_working_directory()):
                            # Someone tried to escape the job directory!
                            is_valid = False

                    # Check that the file exists and isn't a directory
                    if not os.path.exists(self.file_path) or os.path.isdir(self.file_path) or not is_valid:
                        result = Message(Message.RESULT_FAILURE)
                        result.push_string("File {} does not exist on the remote cluster.".format(self.file_path))
                        # Send the result
                        await sock.send(result.to_bytes())
                        # Nothing left to do
                        return
                    else:
                        result = Message(Message.RESULT_OK)
                        self.file = open(self.file_path, "rb")
                        self.file.seek(0, 2)  # move the cursor to the end of the file
                        self.file_size = self.file.tell()
                        result.push_uint(self.file_size)

                    # Send the result
                    await sock.send(result.to_bytes())

                    # Read the next chunk of data from the file
                    # Seek to the correct spot in the file
                    self.file.seek(self.offset)

                    # Check if there is any more file to read
                    while self.offset < self.file_size:
                        # Read this chunk
                        data = self.file.read(self.file_chunk_size)
                        # Update the offset
                        self.offset += self.file_chunk_size

                        # Create a message to send back to the client
                        result = Message(Message.SEND_FILE_CHUNK)
                        result.push_bytes(data)

                        await sock.send(result.to_bytes())

                    # Send the closing chunk
                    result = Message(Message.SEND_FILE_CHUNK)
                    result.push_bytes([])

                    await sock.send(result.to_bytes())
                finally:
                    # Check if there is a file send customisation and call it
                    try:
                        scheduler.file_send_end(self.file_path, self.extra_params)
                    except AttributeError:
                        pass


def create_file_connection(token, settings):
    """
    Creates a new file controller with the specified token

    :param settings: The settings from settings.py
    :param token: The token to use for the connection
    :return: Nothing
    """
    # Create and set the event loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    # Create the file controller
    file_controller = FileController(token, settings)

    # Run the file controller
    asyncio.get_event_loop().run_until_complete(file_controller.run())
