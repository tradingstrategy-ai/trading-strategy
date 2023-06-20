"""Module for custom exceptions. This should contain base classes. Children of these base classes should be defined in the modules where they are used."""

class DataNotFoundError(Exception):
    """Exception raised when data is not found. Currently a base class for ExchangeNotFoundError and PairNotFoundError."""
    
    def __init__(self, message: str):
        super().__init__(message)