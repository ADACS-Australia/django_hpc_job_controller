import asyncio
import logging
from threading import Thread

import websockets

from .file_controller import create_file_connection
from .messaging.message import Message


class JobController:

    def __init__(self, argv, settings):
        self.argv = argv
        self.settings = settings

    async def run(self):
        """
        Called to create a websocket connection to the server and manage incoming messages
        :return: Nothing
        """
        async with websockets.connect('{}/pipe/?token={}'.format(self.settings.HPC_WEBSOCKET_SERVER, self.argv[2])) as sock:
            async for msg in sock:
                print(msg)
                # Create a message from the raw data
                msg = Message(data=msg)
                # Read the message id from the message
                msg_id = msg.pop_uint()

                if msg_id == Message.INITIATE_FILE_CONNECTION:
                    # Create a new thread to handle a file connection
                    Thread(target=create_file_connection, args=[msg.pop_string(), self.settings], daemon=True).start()

                #await sock.send(message)
                else:
                    logging.error("Got unknown message id {}".format(msg_id))

    def execute(self):
        """
        Main entry point of the Job Controller - called by the daemon once it is initialised
        :return: Nothing
        """

        # Start the websocket connection
        asyncio.get_event_loop().run_until_complete(self.run())
