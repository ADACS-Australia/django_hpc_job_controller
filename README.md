# Django HPC Job Controller

---

### NB: ####### Minimum required python is Python 3.6 #######

## Client Setup and Configuration

### Installation Steps

* On the cluster where the client is to run jobs check out the git repository
  * `git clone https://github.com/ASVO-TAO/django_hpc_job_controller.git`
* In the client directory, create a new virtual environment and install the required packages. It is required that the `venv` directory is under the client subdirectory. ie it should be `django_hpc_job_controller/client/venv`
  * `cd django_hpc_job_controller/client`
  * `virtualenv -p python3.6 venv` (Important: The folder must be called venv in the client directory)
  * `. venv/bin/activate`
  * `pip install -r requirements.txt`
* Make a note of the path to the client directory, as it is required when configuring the cluster in the web app admin.
* Configure the settings

### Configuration Options

The client has it's own independent settings that are outside the scope of Django. The client does not use Django at all. The settings file for the client is `client/settings/settings.py`

* `HPC_LOG_DIRECTORY` is the directory where the log files will be emitted. Default: `client/log/`
* `HPC_WEBSOCKET_SERVER` is the fully qualified URL of the websocket server. Default `ws://localhost:8001`. This should follow the format `ws(s)://host:port` with no trailing slash.
* `HPC_SCHEDULER_CLASS` is the class path to the class that inherits from the `scheduler.scheduler.Scheduler` class. Typically you would inherit from the relevant scheduler class to add your own business logic, then set this setting to the path to your inherited class. Typically it is best to put the inherited class in the settings directory. Default `scheduler.local.Local`
* `HPC_JOB_WORKING_DIRECTORY` is the path where the working directory for jobs is. It follows the format `HPC_JOB_WORKING_DIRECTORY/{ui_job_id}/`. Default `/tmp/jobs/`

## Server Setup and Configuration

Currently the best way to install this app in to your project is to either `git clone` the repository in to the root of your Django app, or to add the repository as a `git submodule` (preferred).

Assuming that the `django_hpc_job_controller` repo is checked out in the root of your Django app, do the following to integrate it with your app.

* Add `django_hpc_job_controller` to your `INSTALLED_APPS`
* Install requirements from `django_hpc_job_controller/server/requirements.txt`
* Run migrations `python manage.py migrate`
* Configure settings
* Configure cluster(s)

### Configuration Options

The server uses standard Django settings patterns.

* `HPC_WEBSOCKET_PORT` is the port to listen on for websocket connections. Default `8001`
* `HPC_IPC_UNIX_SOCKET` is the path to the Unix Domain Socket to use for inter-process communication between Django and the websocket server. Websockets used for file transfers will extend this path with a `.` followed by the uuid of the token used for the connection. So for example if `HPC_IPC_UNIX_SOCKET` was `/tmp/job_controller.sock` then Django <---> websocket server communication will be done over `/tmp/job_controller.sock`, while you may see `/tmp/job_controller.sock.xxxx-xxxx-xxxx-xxxx` files used for IPC for the websockets created to transmit files. Default is `/tmp/job_controller.sock`
* `HPC_FILE_CONNECTION_CHUNK_SIZE` is the size of each chunk of a file to be sent over websockets used for file transfers. Default is 1Mb (1024*1024)
* `HPC_JOB_CLASS` is the class path to the job class to use. This exists to define a custom job class that inherits from the built in HpcJob class. Default is `django_hpc_job_controller.models.HpcJob`

### Configure a Cluster

To create a register a new cluster, log in to the Django admin and find the `Hpc Clusters` page. Add a new cluster object, and configure it as follows:

* Set the host name of the cluster, this should be the IP/Domain you would use to manually log in to the cluster over SSH. If this value is `localhost` SSH will not be used to start the daemon, and username/password/key is ignored, instead the `subprocess` module will be used to start the client locally.
* Configure the authentication options, there are 3 available options:-
  * Passphrase protected SSH private key authentication:-
    * Copy the content of the passphrase protected key in to the key field
    * Add the user name to log in to the system with in the user name field
    * Put the passphrase of the SSH key in the password field
  * Unprotected SSH private key authentication:-
    * Copy the content of the private key in to the key field
    * Add the user name to log in to the system with in the user name field
    * Leave the password field blank
  * User name/password authentication:-
    * Leave the key field empty
    * Add the user name to log in to the system with in the user name field
    * Add the users password in to the password field
* Add the path to the client directory on the client. This should be the path to the folder containing `client.py` on the cluster. It should not have a trailing slash, and should not contain `client.py` in the path

## Model Reference

### WebsocketToken

WebsocketToken is the primary model used in authentication and identification of clusters. When the server starts, it generates a UUID for each cluster, then sends that token to the client when it is started. The client then sends that UUID back to the server, the UUID is checked to see if it has been used before, and the client allowed to connect. The server then marks the token used, and the token cannot be reused.

WebsocketToken exports only one function, `send_message` than is used to send a `Message` to the cluster connected with that token.

```python
    def send_message(self, message):
        """
        Sends a message to the websocket associated with this token

        :param message: a Message object to send
        :return: Nothing
        """
```

### HpcCluster

HpcCluster is a record of a client cluster that should be connected when the server is running. 

`HpcCluster.get_ssh_connection`: Returns a paramiko SSH connection to the cluster, based on the host/auth values of the model, that can be used to run arbitrary commands on the remote cluster. This function is primarily used to start the remote client. 

*If using this function, you should take care to close the connection when you're finished using the SSH connection.*

```python
    def get_ssh_connection(self):
        """
        Returns a Paramiko SSH connection to the cluster

        :return: The SSH instance
        """
```

`HpcCluster.try_connect`:   Checks that this cluster is online, and if not, attempts to start the remote client on the cluster. This function checks internally if the hostname of the cluster is `localhost`, and if so uses subprocess rather than an SSH connection.

```python
    def try_connect(self, force=False):
        """
        Checks if this cluster is connected, and if not tries to connect

        :param force: Forces the remote client to be started even if the cluster reports that it is online
        :return: Nothing
        """
```

`HpcCluster.is_connected`: Checks if the cluster is online or not. This function is cheap, and has low overhead. It is the preferred function to call for checking if the cluster is online from within Django.

```python
    def is_connected(self):
        """
        Checks if this cluster is currently online

        :return: A WebsocketToken object if the cluster is online otherwise None
        """
```

`HpcCluster.fetch_remote_file`: Is used to initiate and return a file from the remote cluster. This function returns a StreamingHTTPResponse that can be streamed directly as a response to a request in a Django view. The path parameter is the absolute path to a remote file on the cluster if the UI ID parameter is none. If the UI ID is set, then the path is relative to the Job's remote output directory. If force download is true, then the StreamingHTTPResponse forces the browser to treat the file as an attachment and download the file.

```python
    def fetch_remote_file(self, path, ui_id=None, force_download=True):
        """
        Fetches a file, path, from a this cluster over a websocket connection, and returns a Streaming HTTP response

        :param force_download: If the returned Streaming HTTP Response should force a download
        :param ui_id: The UI ID of the job to fetch the file for, if this is None files can be fetched from anywhere
        :param path: The path to the file to fetch

        :return: A Django StreamingHTTPResponse
        """
```

### HpcJob

`HpcJob.choose_cluster`: This function is called when a job is submitted. This function should be overridden in the inherited model and used to choose which cluster the job should be submitted to. This function takes the job parameters as it's parameter.

```python
    def choose_cluster(self, parameters):
        """
        Chooses the most appropriate cluster for the job to run on

        Should be overridden

        :type parameters: Job parameters that may be used to choose a correct cluster for this job

        :return: The cluster chosen to run the job on
        """
```

`HpcJob.fetch_remote_file_list`: This function returns a list of files in the output directory for the job. The path is a path relative to the root of the output directory of the job. Files may be fetched recursively or just within the path specified. This function returns a `GET_FILE_TREE` Message, with the format:

```python
# uint: Number of files/folders
    # string: Path (relative to job working directory)
    # bool: Is folder
    # ulong: File size if file else 0
```

```python
    def fetch_remote_file_list(self, path="/", recursive=True):
        """
        Retrieves the list of files at the specified relative path and returns it

        :param path: The relative path in the job output directory to fetch the file list for
        :param recursive: If the result should be the recursive list of files
        :return: A recursive dictionary of file information
        """
```

`HpcJob.fetch_remote_file`: Returns a file from the remote cluster relative to the output path of the job. This function leverages the functionality of `HpcCluster.fetch_remote_file`

```python
    def fetch_remote_file(self, path, force_download=True):
        """
        Retreives a file from the remote job working directory specified with the relative directory path

        :param force_download: If the returned Streaming HTTP Response should force a download
        :param path: The relative path to the job working directory of the file to retreive
        :return: A streaming HTTP response
        """
```

`HpcJob.submit`: Submits the job with the specified parameters. The parameters can be any picklable object. This function will call `choose_cluster` to choose the cluster, then submit the job to that cluster. When the job is submitted, the job will enter PENDING state if the cluster is not online. If the cluster is online (or comes online) the job will enter SUBMITTING state. Once the cluster acknowledges that the job has been submitted the job state is updated to SUBMITTED.

```python
    def submit(self, parameters):
        """
        Submits this job to the cluster

        Should not be overridden

        :param parameters: Any python picklable object containing the information to be sent to the client

        :return: The current status of the job (Either SUBMITTED or QUEUED)
        """
```

`HpcJob.cancel`: Cancels the job on the remote cluster if it is in a cancellable state. The job will enter CANCELLING state until the client acknowledges that it has cancelled the job, when the job will then enter CANCELLED state.

```python
    def cancel(self):
        """
        Cancels the job on the cluster

        :return: Nothing
        """
```

`HpcJob.delete_job`: Deletes the job data from the remote cluster if the job is in a deletable state. The Job will enter DELETING state until the client acknowledges that it has deleted the job, when the job will then enter DELETE state.

```python
    def delete_job(self):
        """
        Deletes the job and removes the data on the cluster

        :return: Nothing
        """

