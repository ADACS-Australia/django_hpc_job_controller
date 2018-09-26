import asyncio
import logging
import pickle
from threading import Thread

import websockets

from .file_controller import create_file_connection
from .messaging.message import Message


class JobController:

    def __init__(self, argv, settings):
        self.argv = argv
        self.settings = settings
        self.sock = None
        self.queue = asyncio.Queue()

    async def handle_message(self, message):
        # Convert the raw message to a Message object
        msg = Message(data=message)

        # Get the message id
        msg_id = msg.pop_uint()

        # Handle the message
        if msg_id == Message.INITIATE_FILE_CONNECTION:
            # Create a new thread to handle a file connection
            Thread(target=create_file_connection, args=[msg.pop_string(), self.settings], daemon=True).start()
        elif msg_id == Message.SUBMIT_JOB:
            print("Job is getting submitted")
            hpc_job_id = msg.pop_uint()
            job_params = pickle.loads(msg.pop_bytes())

            # Check if this job has already been submitted by us
                # If not, submit the job and record that we have submitted the job
                # If so, check the state of the job and notify the server of it's current state

            print("HpcJob ID:", hpc_job_id)
            print("Job params:", job_params)

            # Acknowledge the submission of the job
            result = Message(Message.SUBMIT_JOB)
            result.push_uint(hpc_job_id)
            # Send the result
            await self.sock.send(result.to_bytes())
        # await sock.send(message)
        else:
            logging.info("Got unknown message id {}".format(msg_id))

    async def send_handler(self):
        """
        Handles sending messages from the queue to the server

        :return: Nothing
        """
        # Automatically handles sending messages added to the queue
        while True:
            # Wait for a message from the queue
            message = await self.queue.get()
            # Send the message
            await self.sock.send(message)

    async def recv_handler(self):
        """
        Handles receiving messages from the client

        :return: Nothing
        """
        # Loop forever
        while True:
            # Wait for a message to arrive on the websocket
            message = await self.sock.recv()
            # Handle the message
            await self.handle_message(message)

    async def run(self):
        """
        Called to create a websocket connection to the server and manage incoming messages
        :return: Nothing
        """

        self.sock = await websockets.connect(
            '{}/pipe/?token={}'.format(self.settings.HPC_WEBSOCKET_SERVER, self.argv[2]))

        # Create the consumer and producer tasks
        consumer_task = asyncio.ensure_future(self.recv_handler())
        producer_task = asyncio.ensure_future(self.send_handler())

        # Wait for one of the tasks to finish
        done, pending = await asyncio.wait(
            [consumer_task, producer_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        # Kill the remaining tasks
        for task in pending:
            task.cancel()

    def execute(self):
        """
        Main entry point of the Job Controller - called by the daemon once it is initialised
        :return: Nothing
        """

        # Start the websocket connection
        asyncio.get_event_loop().run_until_complete(self.run())
