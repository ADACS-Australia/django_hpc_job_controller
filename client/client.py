import logging
import logging.handlers
import os
import signal
import sys
import traceback

from core.job_controller import JobController
from settings import settings
from utils.daemon import Daemon


class JobControllerDaemon(Daemon):
    def __init__(self, pid_file, std_in='/dev/null', std_out='/dev/null', std_err='/dev/null', argv=[]):
        """
        Class constructor
        :param pid_file: The path to the process id file (from settings.py)
        """
        # Call the super constructor
        super().__init__(pid_file, std_in, std_out, std_err)

        # Set globals
        self.logger = None
        self.sleep_time = None
        self.controller = None
        self.argv = argv

    def prepare_log_file(self):
        """
        Creates the log file and sets up logging parameters
        :return: None
        """
        # Get the log file name
        log_file_name = os.path.join(settings.LOG_DIRECTORY, 'logfile.log')

        # Make sure that the log directory exists
        os.makedirs(settings.LOG_DIRECTORY, exist_ok=True)

        # Create the logger
        self.logger = logging.getLogger()
        self.logger.setLevel(logging.DEBUG)

        # Create the log handler
        handler = logging.handlers.RotatingFileHandler(log_file_name, maxBytes=10485760, backupCount=5)
        handler.setFormatter(logging.Formatter('%(asctime)s - %(message)s'))

        # Add the handler to the logger
        self.logger.addHandler(handler)

    def run(self):
        """
        The main entry point of the daemon
        :return: Nothing
        """

        # Make sure the log file is set up
        self.prepare_log_file()

        # Create a new start up entry in the log file
        logging.info('-----------------------------------------------------------------')
        logging.info('Job Controller starting...')

        # Create the Job Controller
        self.controller = JobController(self.argv, settings)

        try:
            # Log that the daemon is running
            logging.info("Job Controller is Active")
            self.controller.execute()
        except Exception as Exp:
            # An exception occurred, log the exception to the log
            logging.error("Error In Main")
            logging.error(type(Exp))
            logging.error(Exp.args)
            logging.error(Exp)

            # Also log the stack trace
            exc_type, exc_value, exc_traceback = sys.exc_info()
            lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(''.join('!! ' + line for line in lines))

        logging.info('Job Controller stopping - lost connection to remote server')
        logging.info('-----------------------------------------------------------------')

    @staticmethod
    def handle_exit(signum, frame):
        # Unexpected exit
        logging.info('I received Exit Code')
        # Return success status
        sys.exit(0)


# Entry point for the main daemon system.
if __name__ == '__main__':
    # Create the daemon instance
    daemon = JobControllerDaemon(
        settings.DAEMON_PID_FILE,
        '/dev/null',
        os.path.join(settings.LOG_DIRECTORY, 'out.log'),
        os.path.join(settings.LOG_DIRECTORY, 'err.log'),
        sys.argv
    )

    # Register an exit handler to catch unexpected shutdowns
    signal.signal(signal.SIGTERM, daemon.handle_exit)

    # Check that the right number of arguments were provided to the daemon on the command line
    if len(sys.argv) == 3:
        # Yes, check the command
        if 'start' == sys.argv[1]:
            # Start the daemon
            print('Starting Job Controller')
            daemon.start()
        else:
            # Unknown command
            logging.error("Unknown command")
            # Exit with error status
            sys.exit(-1)
    elif len(sys.argv) == 2:
        if 'stop' == sys.argv[1]:
            # Stop the daemon
            print('Stopping Job Controller')
            daemon.stop()
        else:
            # Unknown command
            logging.error("Unknown command")
            # Exit with error status
            sys.exit(-1)

        # Exit with success
        sys.exit(0)
    else:
        # No, print correct usage
        print("usage: %s start [websocket token] | stop" % sys.argv[0])

        # Exit with error status
        sys.exit(-1)
