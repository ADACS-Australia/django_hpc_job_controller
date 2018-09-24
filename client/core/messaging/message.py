import struct


class Message:
    """
    The Message class is an easy to use push/pop binary messaging framework that allows pushing and popping basic
    primitive types
    """
    # System message id's
    # Used internally to represent a success condition
    # ...: Any arbitrary content may follow
    RESULT_OK = 0

    # Used internally to represent a failure condition
    # string: Failure reason
    RESULT_FAILURE = 1

    # Used internally to check if a cluster is currently connected or not
    # uint: The django db id of the cluster to check
    IS_CLUSTER_ONLINE = 2

    # Used internally to send a message over the django/websocket server unix domain socket
    # string: The token representing the websocket connection to send the message over
    # bytes: The message to send over the websocket
    TRANSMIT_WEBSOCKET_MESSAGE = 3

    # Creates a new file controller websocket connection for transmitting a file
    # string: websocket token id
    INITIATE_FILE_CONNECTION = 1000

    # Sets the file that the remote file connection is to read
    # string: path to file on remote machine
    SET_FILE_CONNECTION_FILE_NAME = 1001

    # Sends a file chunk to the server
    # bytes: The data for this chunk - if the len is 0 then there are no more chunks
    SEND_FILE_CHUNK = 1002

    def __init__(self, msg_id=None, data=None):
        """
        Message constructor - creates a new message with the provided message id, or creates an existing message from
        the provided data

        :param msg_id: The ID of the message if creating a new message (int32)
        :param data: The data of the message if creating a message from existing data (bytearray)
        :return:
        """

        # Verify that at least one of the data parameters were provided
        if msg_id is None and data is None:
            # Doh
            raise Exception("Can't create a message with neither a message id or initial data")

        # Check that both parameters were not provided
        if msg_id and data:
            # Doh
            raise Exception("Can't create a message with both a message id and initial data")

        # Check if this is a new message
        if msg_id is not None:
            # Initialise the data array
            self.data = bytearray()

            # Reset the offset to the start of the message
            self.offset = 0

            # Yes, push the message id on to the data
            self.push_uint(msg_id)
        else:
            # Set the data
            self.data = data

            # Reset the offset to the start of the message
            self.offset = 0


    def push_bool(self, i):
        """
        Pushes a boolean value on to the message
        :param i: The boolean to push
        :return: Nothing
        """
        if i:
            self.push_ubyte(1)
        else:
            self.push_ubyte(0)

    def pop_bool(self):
        """
        Pops a bool from the data and returns it
        :return: The bool at the current message offset
        """
        return self.pop_ubyte() == 1

    def push_ubyte(self, i):
        """
        Pushes a single byte on to the message
        :param i: The byte to push
        :return: Nothing
        """
        self.data.extend(struct.pack('B', i))

    def pop_ubyte(self):
        """
        Pops a byte from the data and returns it
        :return: The byte at the current message offset
        """
        self.offset += 1
        return struct.unpack_from('B', self.data, self.offset-1)[0]

    def push_byte(self, i):
        """
        Pushes a single byte on to the message
        :param i: The byte to push
        :return: Nothing
        """
        self.data.extend(struct.pack('b', i))

    def pop_byte(self):
        """
        Pops a byte from the data and returns it
        :return: The byte at the current message offset
        """
        self.offset += 1
        return struct.unpack_from('b', self.data, self.offset-1)[0]

    def push_ushort(self, i):
        """
        Pushes a single short on to the message
        :param i: The short to push
        :return: Nothing
        """
        self.data.extend(struct.pack('H', i))

    def pop_ushort(self):
        """
        Pops a short from the data and returns it
        :return: The short at the current message offset
        """
        self.offset += 2
        return struct.unpack_from('H', self.data, self.offset-2)[0]

    def push_short(self, i):
        """
        Pushes a single short on to the message
        :param i: The short to push
        :return: Nothing
        """
        self.data.extend(struct.pack('h', i))

    def pop_short(self):
        """
        Pops a short from the data and returns it
        :return: The short at the current message offset
        """
        self.offset += 2
        return struct.unpack_from('H', self.data, self.offset-2)[0]

    def push_uint(self, i):
        """
        Pushes a single int on to the message
        :param i: The int to push
        :return: Nothing
        """
        self.data.extend(struct.pack('I', i))

    def pop_uint(self):
        """
        Pops a int from the data and returns it
        :return: The int at the current message offset
        """
        self.offset += 4
        return struct.unpack_from('I', self.data, self.offset-4)[0]

    def push_int(self, i):
        """
        Pushes a single int on to the message
        :param i: The int to push
        :return: Nothing
        """
        self.data.extend(struct.pack('i', i))

    def pop_int(self):
        """
        Pops a int from the data and returns it
        :return: The int at the current message offset
        """
        self.offset += 4
        return struct.unpack_from('i', self.data, self.offset-4)[0]

    def push_ulong(self, i):
        """
        Pushes a single long on to the message
        :param i: The long to push
        :return: Nothing
        """
        self.data.extend(struct.pack('Q', i))

    def pop_ulong(self):
        """
        Pops a long from the data and returns it
        :return: The long at the current message offset
        """
        self.offset += 8
        return struct.unpack_from('Q', self.data, self.offset-8)[0]

    def push_long(self, i):
        """
        Pushes a single long on to the message
        :param i: The long to push
        :return: Nothing
        """
        self.data.extend(struct.pack('q', i))

    def pop_long(self):
        """
        Pops a long from the data and returns it
        :return: The long at the current message offset
        """
        self.offset += 8
        return struct.unpack_from('q', self.data, self.offset-8)[0]

    def push_float(self, i):
        """
        Pushes a single byte on to the message
        :param i: The byte to push
        :return: Nothing
        """
        self.data.extend(struct.pack('f', i))

    def pop_float(self):
        """
        Pops a float from the data and returns it
        :return: The float at the current message offset
        """
        self.offset += 4
        return struct.unpack_from('f', self.data, self.offset-4)[0]

    def push_double(self, i):
        """
        Pushes a single byte on to the message
        :param i: The byte to push
        :return: Nothing
        """
        self.data.extend(struct.pack('d', i))

    def pop_double(self):
        """
        Pops a byte from the data and returns it
        :return: The byte at the current message offset
        """
        self.offset += 8
        return struct.unpack_from('d', self.data, self.offset-8)[0]

    def push_string(self, s):
        """
        Pushes a single byte on to the message
        :param i: The byte to push
        :return: Nothing
        """
        # Push the length of the string
        self.push_uint(len(s))

        # Map the characters to ints and add them to the data
        for c in s:
            self.push_uint(ord(c))

    def pop_string(self):
        """
        Pops a string from the data and returns it
        :return: The string at the current message offset
        """
        # Get the length of the string
        string_len = self.pop_uint()

        # Iterate over each character and append it to the array
        result = ''
        for _ in range(string_len):
            result += chr(self.pop_uint())

        # Return the array
        return result

    def push_bytes(self, d):
        """
        Pushes a single byte on to the message
        :param i: The byte to push
        :return: Nothing
        """
        # Push the length of the data
        self.push_uint(len(d))

        # Push the data
        self.data.extend(d)

    def pop_bytes(self):
        """
        Pops an array of bytes from the data and returns it
        :return: The byte array at the current message offset
        """
        # Get the length of the bytes
        byte_len = self.pop_uint()

        # Update the offset
        self.offset += byte_len

        # Slice the array and return the data
        return self.data[self.offset-byte_len:self.offset]

    def to_bytes(self):
        """
        Returs the data array
        :return: The data array
        """
        # Convert the data to bytes and return it
        return bytes(self.data)

    def size(self):
        return len(self.data)