import os


class EnvironmentContext(object):
    """Context manager for creating a temporary environment variable.
    https://gist.github.com/sidprak/a3571943bcf6df0565c09471ab2f90b8
    """
    def __init__(self, key, value):
        self.key = key
        self.newValue = value

    def __enter__(self):
        self.oldValue = os.environ.get(self.key)
        os.environ[self.key] = self.newValue

    def __exit__(self, *args):
        if self.oldValue:
            os.environ[self.key] = self.oldValue
        else:
            del os.environ[self.key]
