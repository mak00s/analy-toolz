"""Custom Exceptions"""


class OneLineException(Exception):
    """base class for exceptions whose messages will be displayed on a single
    line for better readability in Cloud Function Logs"""

    def __init__(self, msg):
        super().__init__(msg.replace('\n', ' ').replace('\r', ''))


class PartialDataReturned(OneLineException):
    """Exception to indicate that partial data was returned from API.
    This occurs when clientId is included in the request's dimensions.
    clientId is not officially supported by Google. Using this dimension in an
    Analytics report may thus result in unexpected & unexplainable behavior (such as
    restricting the report to exactly 10,000 or 10,001 rows)."""

    def __init__(self, message="Partial data returned from API: This occurs when clientId is included in the "
                               "request's dimensions."):
        self.message = message
        super().__init__(self.message)


class ApiDisabledException(OneLineException):
    """Exception to indicate that an API is not enabled for the GCP project"""
