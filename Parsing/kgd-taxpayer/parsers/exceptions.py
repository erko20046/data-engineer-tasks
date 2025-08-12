class DataNotFoundException(Exception):
    def __str__(self):
        return 'Data is not found'

class AuthorizationFailed(Exception):
    def __str__(self):
        return 'Authorization failed'

class EmptyResponseSourceException(Exception):
    def __str__(self):
        return 'Empty response from source'

class InconsistentData(Exception):
    def __str__(self):
        return 'First and last name are required'

class CorruptedJsonException(Exception):
    def __str__(self):
        return 'Received corrupted JSON data'

class WrongDataFormatException(Exception):
    def __init__(self, format_type: str):
        super().__init__(f'Received data is not in the expected format {format_type}')

class InconsistentRequest(Exception):
    def __str__(self):
        return 'Invalid type or uin'