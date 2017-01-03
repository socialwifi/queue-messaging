class BaseExceptionWithPayload(Exception):
    default_message = ''

    def __init__(self, message=None, **kwargs):
        args = ', '.join(['%s=%s' % (key, value) for key, value in kwargs.items()])
        super().__init__(message or self.default_message, args)


class QueueClientError(BaseExceptionWithPayload):
    default_message = 'Error in queue client.'


class PubSubError(QueueClientError):
    default_message = 'Error in pubsub service.'


class QueueMessagingError(BaseExceptionWithPayload):
    default_message = 'Error in queue messaging.'


class EncodingError(BaseExceptionWithPayload):
    default_message = 'Error while encoding data.'


class DecodingError(BaseExceptionWithPayload):
    default_message = 'Error while decoding data.'


class ConfigurationError(Exception):
    pass


class NoMessagesReceivedError(Exception):
    pass
