class BaseException(Exception):
    pass

class EntryExistsError(BaseException):
    def __init__(self, entry):
        self.entry = entry

    def __str__(self):
        return '{}'.format(self.entry)

class NoEntryExistsError(BaseException):
    def __init__(self, entry):
        self.entry = entry

    def __str__(self):
        return '{}'.format(self.entry)

class InvalidDockerCredentials(BaseException):
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return '{}'.format(self.error.explanation)

class InvalidDockerImage(BaseException):
    def __init__(self, image):
        self.image = image

    def __str__(self):
        return '{}'.format(self.image.explanation)

class NullImage(BaseException):
    def __init__(self, error):
        self.error = error

    def __str__(self):
        return '{}'.format(self.error)

