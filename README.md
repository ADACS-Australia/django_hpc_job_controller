# Django HPC Job Controller

---

## Client Setup and Configuration

### Installation Steps

* On the cluster where the client is to run jobs check out the git repository
  * `git clone https://github.com/ASVO-TAO/django_hpc_job_controller.git`
* In the client directory, create a new virtual environment and install the required packages. It is required that the `venv` directory is under the client subdirectory. ie it should be `django_hpc_job_controller/client/venv`
  * `cd django_hpc_job_controller/client`
  * `virtualenv -p python3.6 venv`
  * `. venv/bin/activate`
  * `pip install -r requirements.txt`
* Make a note of the path to the client directory, as it is required when configuring the cluster in the web app admin.
* Configure the settings

### Configuration Options

The client has it's own independent settings that are outside the scope of Django. The client does not use Django at all. The settings file for the client is `client/settings/settings.py`

* `HPC_LOG_DIRECTORY` is the directory where the log files will be emitted. Default: `client/log/`
* `HPC_DAEMON_PID_FILE` is the path to the process id used by the daemon. Default: `/tmp/job_controller.pid`
* `HPC_WEBSOCKET_SERVER` is the fully qualified URL of the websocket server. Default `ws://localhost:8001`. This should follow the format `ws(s)://host:port` with no trailing slash.

## Server Setup and Configuration

Currently the best way to install this app in to your project is to either `git clone` the repository in to the root of your Django app, or to add the repository as a `git submodule`.

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

### Configure a Cluster

To create a register a new cluster, log in to the Django admin and find the `Hpc Clusters` page. Add a new cluster object, and configure it as follows:

* Set the host name of the cluster, this should be the IP/Domain you would use to manually log in to the cluster over SSH
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
* Set the batch scheduler type for this cluster

## API and Examples

