import logging

from cached_property import cached_property

from queue_messaging import configuration
from queue_messaging import exceptions
from queue_messaging.data import encoding
from queue_messaging.data import structures
from queue_messaging.services import pubsub


logger = logging.getLogger(__name__)


class Envelope:
    def __init__(self, pulled_message, client, dead_letter_client,
                 type_to_model):
        self._pulled_message = pulled_message
        self._client = client
        self._dead_letter_client = dead_letter_client
        self._type_to_model = type_to_model

    def acknowledge(self):
        logger.debug('Message ACK')
        self._pulled_message.ack()

    @cached_property
    def model(self):
        if self._pulled_message is None:
            raise exceptions.NoMessagesReceivedError
        return encoding.decode_payload(
            header=self.header,
            encoded_data=self._pulled_message.data,
            message_config=self._type_to_model)

    @cached_property
    def header(self) -> structures.Header:
        return encoding.create_header(self._pulled_message.attributes)

    def mark_as_dead_letter(self):
        self._send_to_dead_letter_queue()
        self.acknowledge()

    def _send_to_dead_letter_queue(self):
        message = self._pulled_message.data
        attributes = self._pulled_message.attributes
        try:
            self._dead_letter_client.send(message=message, **attributes)
        except exceptions.QueueClientError as e:
            raise exceptions.QueueMessagingError(
                'Error while sending a message to the dead letter queue',
                data=message,
                attributes=attributes,
                error=e,
            )


class Messaging:
    def __init__(self, client, dead_letter_client, type_to_model):
        self._client = client
        self._dead_letter_client = dead_letter_client
        self._type_to_model = type_to_model

    @classmethod
    def create_from_dict(cls, dict):
        config = configuration.Factory(dict).create()
        client = pubsub.get_pubsub_client(config)
        dead_letter_client = pubsub.get_fallback_pubsub_client(config)
        type_to_model = cls._create_type_mapping(config.MESSAGE_TYPES)
        return cls(client, dead_letter_client, type_to_model)

    @staticmethod
    def _create_type_mapping(types):
        type_to_model = {}
        for model_class in types:
            try:
                type_name = model_class.Meta.type_name
            except AttributeError:
                raise exceptions.ConfigurationError(
                    'Expected class with Meta.type_name: {}'.format(model_class)
                )
            if type_name in type_to_model:
                raise exceptions.ConfigurationError(
                    'Multiple models defined for type: {}'.format(type_name)
                )
            type_to_model[type_name] = model_class
        return type_to_model

    def send(self, model: structures.Model):
        attributes = self._get_attributes(model)
        message = self._get_message(model)
        self._send_message(message, attributes)

    def receive(self, callback):
        self._pull_message(lambda message: callback(self._wrap_in_envelope(message)))

    def _wrap_in_envelope(self, pulled_message):
        return Envelope(
            pulled_message=pulled_message,
            client=self._client,
            dead_letter_client=self._dead_letter_client,
            type_to_model=self._type_to_model
        )

    def _get_attributes(self, model: structures.Model):
        return encoding.create_attributes(model)

    def _get_message(self, model):
        return encoding.encode(model)

    def _send_message(self, message, attributes):
        try:
            self._client.send(message=message, **attributes)
        except exceptions.QueueClientError as e:
            raise exceptions.QueueMessagingError(
                'Error while sending a message',
                attributes=attributes,
                model=message,
                error=e,
            )

    def _pull_message(self, callback):
        try:
            return self._client.receive(callback)
        except exceptions.QueueClientError as e:
            raise exceptions.QueueMessagingError(
                'Error while receiving a message',
                error=e,
            )
