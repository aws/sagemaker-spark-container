import logging
import pathlib
import sys

EXIT_CODE_SUCCESS = 0
EXIT_CODE_ALGORITHM_ERROR = 1


class BaseError(Exception):
    """Abstract base for all errors that may cause the container to exit/terminate
    unsuccessfully. All direct sub-classes should be kept/maintained in this file.

    Args: see `Attributes` below.

    Attributes:
        message     (string): Description of why this exception was raised.
        caused_by   (exception): The underlying exception that caused this
            exception to be raised. This should be a non-BaseSdkError.
        exit_code   (int): The exit code that should be used if this exception
            makes it way to the top-level handler.
        failure_prefix (string): Prefix for the job failure status if
            this exception is handled at the top-level. This will be seen by the
            user in the Console UI.
    """

    message_path = "/opt/ml/output/message"

    def __init__(
        self, message: str, caused_by: Exception, exit_code: int = 127, failure_prefix: str = "Algorithm Error",
    ):
        formatted_message = BaseError._format_exception_message(failure_prefix, message, caused_by)
        super(BaseError, self).__init__(formatted_message)
        self.logger = logging.getLogger("smspark-submit")
        self.message = formatted_message
        self.caused_by = caused_by
        self.failure_prefix = failure_prefix
        self.exit_code = exit_code

    @staticmethod
    def _format_exception_message(failure_prefix: str, message: str, caused_by: Exception) -> str:
        """Generate the exception message."""
        cause_name = caused_by.__class__.__name__
        cause_message = str(caused_by)
        formatted_message = f"{failure_prefix}: (caused by {cause_name}): {message}: {cause_message}"
        # up to 1KB is allowed in output message
        return formatted_message[:1024].strip()

    def log_and_exit(self) -> None:
        """Write message to output file and exit."""
        path = pathlib.Path(BaseError.message_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(str(path), "w") as output_message_file:
            output_message_file.write(self.message)
        self.logger.info(f"exiting with code {self.exit_code}: {self.message}")
        sys.exit(self.exit_code)


class AlgorithmError(BaseError):
    """Exception used to indicate a problem that occurred with the algorithm."""

    def __init__(self, message: str, caused_by: Exception, exit_code: int = EXIT_CODE_ALGORITHM_ERROR):
        super(AlgorithmError, self).__init__(message, caused_by, failure_prefix="Algorithm Error", exit_code=exit_code)


class InputError(AlgorithmError):
    """Exception used to indicate that the customer's input caused spark-submit to fail."""

    def __init__(self, caused_by: Exception) -> None:
        super(InputError, self).__init__(message="error preparing for spark submit", caused_by=caused_by)
