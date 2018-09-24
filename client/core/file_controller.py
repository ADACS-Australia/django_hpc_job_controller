import asyncio
import logging
import os

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

    async def run(self):
        """
        Called to create a file websocket connection to the server and manage incoming messages

        :return: Nothing
        """
        async with websockets.connect('{}/file/?token={}'.format(self.settings.HPC_WEBSOCKET_SERVER, self.token),
                                      max_size=2 ** 32) as sock:
            logging.info("File controller connected ok with token {}".format(self.token))
            async for msg in sock:
                # Convert the data to a message
                msg = Message(data=msg)

                # Read the message id
                msg_id = msg.pop_uint()

                # Handle the message
                if msg_id == Message.SET_FILE_CONNECTION_FILE_DETAILS:
                    # Read the file name from the message
                    self.file_path = msg.pop_string()
                    self.file_chunk_size = msg.pop_ulong()

                    # Check that the file exists
                    if not os.path.exists(self.file_path):
                        result = Message(Message.RESULT_FAILURE)
                        result.push_string("File {} does not exist on the remote cluster.".format(self.file_path))
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


def create_file_connection(token, settings):
    """
    Creates a new file controller with the specified token

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
