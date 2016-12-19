class BaseExceptionWithPayload(Exception):
    default_message = ''

    def __init__(self, message=None, **kwargs):
        args = ', '.join(['%s=%s' % (key, value) for key, value in kwargs.items()])
        super().__init__(message or self.default_message, args)


class EncodingError(BaseExceptionWithPayload):
    default_message = 'Error while encoding data.'


class DecodingError(BaseExceptionWithPayload):
    default_message = 'Error while decoding data.'
