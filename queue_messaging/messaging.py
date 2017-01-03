import logging

from queue_messaging import configuration
from queue_messaging import exceptions
from queue_messaging.data import encoding
from queue_messaging.data import structures
from queue_messaging.services import pubsub


logger = logging.getLogger(__name__)


class Envelope:
    def __init__(self, pulled_message, client, dead_letter_client,
                 types_to_model):
        self._pulled_message = pulled_message
        self._client = client
        self._dead_letter_client = dead_letter_client
        self._types_to_model = types_to_model

    def acknowledge(self):
        acknowledge_id = self._pulled_message.ack_id
        self._client.acknowledge(acknowledge_id)

    @property
    def model(self):
        if self._pulled_message is None:
            raise exceptions.NoMessagesReceivedError
        return encoding.decode_payload(
            header=encoding.create_header(self._pulled_message.attributes),
            encoded_data=self._pulled_message.data,
            message_config=self._types_to_model)

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
    def __init__(self, config: configuration.Configuration):
        self._client = pubsub.get_pubsub_client(config)
        self._dead_letter_client = pubsub.get_fallback_pubsub_client(config)
        self._types_to_model, self._models_to_type = (
            self._create_types_mappings(config.MESSAGE_TYPES))

    @staticmethod
    def _create_types_mappings(types):
        types_to_model = {}
        models_to_type = {}
        for model_class in types:
            try:
                type_name = model_class.Meta.type_name
            except AttributeError:
                raise exceptions.ConfigurationError(
                    'Expected class with Meta.type_name: {}'.format(model_class)
                )
            if type_name in types_to_model:
                raise exceptions.ConfigurationError(
                    'Multiple models defined for type: {}'.format(type_name)
                )
            types_to_model[type_name] = model_class
            if model_class in models_to_type:
                raise exceptions.ConfigurationError(
                    'Multiple types defined for model: {}'.format(model_class)
                )
            models_to_type[model_class] = type_name
        return types_to_model, models_to_type

    @classmethod
    def create_from_dict(cls, dict):
        return cls(config=configuration.Factory(dict).create())

    def send(self, model: structures.Model):
        attributes = self._get_attributes(model)
        message = self._get_message(model)
        self._send_message(message, attributes)

    def receive(self) -> Envelope:
        pulled_message = self._pull_message()
        return Envelope(
            pulled_message=pulled_message,
            client=self._client,
            dead_letter_client=self._dead_letter_client,
            types_to_model=self._types_to_model
        )

    def _get_attributes(self, model: structures.Model):
        return encoding.create_attributes(model, self._models_to_type)

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

    def _pull_message(self):
        try:
            return self._client.receive()
        except exceptions.QueueClientError as e:
            raise exceptions.QueueMessagingError(
                'Error while receiving a message',
                error=e,
            )
