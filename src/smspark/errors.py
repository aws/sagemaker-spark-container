EXIT_CODE_SUCCESS = 0
EXIT_CODE_ALGORITHM_ERROR = 1
EXIT_CODE_CUSTOMER_ERROR = 2
EXIT_CODE_PLATFORM_ERROR = 3


class BaseError(Exception):
    """Abstract base for all errors that may cause an algorithm to exit/terminate
    unsuccessfully. All direct sub-classes should be kept/maintained in this file.

    These errors are grouped into two categories:

        1. AlgorithmError: an unexpected or unknown failure that cannot be
                           avoided by the customer and is due to a bug in
                           the algorithm.

        2. CustomerError:  a failure which can be prevented/avoided by the
                           customer (e.g. change mini_batch_size).

    All other types of errors/exceptions should be converted by default to an
    AlgorithmError.

    These classes are also responsible for providing the exit behaviour/code,
    the failure reason to output for the training service, and the log messages
    that should be printed upon termination.

    Args: see `Attributes` below.

    Attributes:
        message     (string): Description of why this exception was raised.
        caused_by   (exception): The underlying exception that caused this
            exception to be raised. This should be a non-BaseSdkError.
        exit_code   (int): The exit code that should be used if this exception
            makes it way to the top-level handler.
        failure_prefix (string): Prefix for the training job failure status if
            this exception is handled at the top-level. This will be seen by the
            user in the Console UI.
    """

    def __init__(self, message=None, caused_by=None, exit_code=127, failure_prefix="Algorithm Error"):
        formatted_message = BaseError._format_exception_message(message, caused_by)
        super(BaseError, self).__init__(formatted_message)
        self.message = formatted_message
        self.caused_by = caused_by
        self.failure_prefix = failure_prefix
        self.exit_code = exit_code

    @staticmethod
    def _format_exception_message(message, caused_by):
        """Generates the exception message.

        If a message has been explicitly passed then we use that as the exception
        message. If we also know the underlying exception type we prepend that
        to the name.

        If there is no message but we have an underlying exception then we use
        that exceptions message and prepend the type of the exception.
        """
        if message:
            formatted_message = message
        elif caused_by:
            formatted_message = getattr(caused_by, "message", str(caused_by))
        else:
            formatted_message = "unknown error occurred"

        if caused_by:
            formatted_message += " (caused by {})".format(caused_by.__class__.__name__)

        return formatted_message

    def failure_message(self) -> str:
        """Message to print to the customer in the job failure reason."""
        message = "{}: {}".format(self.failure_prefix, self.message)

        if self.caused_by:
            message += "\n\nCaused by: {}".format(self.caused_by)

        return message


class AlgorithmError(BaseError):
    """Exception used to indicate a problem that occurred with the algorithm."""

    def __init__(self, message=None, caused_by=None):
        super(AlgorithmError, self).__init__(
            message, caused_by, failure_prefix="Algorithm Error", exit_code=EXIT_CODE_ALGORITHM_ERROR
        )


class CustomerError(BaseError):
    """Exception used to indicate a problem caused by mis-configuration or other customer input."""

    def __init__(self, message=None, caused_by=None):
        super(CustomerError, self).__init__(
            message, caused_by, failure_prefix="Customer Error", exit_code=EXIT_CODE_CUSTOMER_ERROR
        )
