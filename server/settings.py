from django.conf import settings

# The port to listen on for websocket connections
HPC_WEBSOCKET_PORT = getattr(settings, 'HPC_WEBSOCKET_PORT', 8001)

# The path to the unix socket for
HPC_IPC_UNIX_SOCKET = getattr(settings, 'HPC_IPC_UNIX_SOCKET', '/tmp/job_controller.sock')

# The size of each chunk to send over the file websocket
HPC_FILE_CONNECTION_CHUNK_SIZE = getattr(settings, 'HPC_FILE_CONNECTION_CHUNK_SIZE', 1024*1024)