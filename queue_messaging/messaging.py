import logging

from queue_messaging import configuration
from queue_messaging import exceptions
from queue_messaging.data import encoding
from queue_messaging.data import structures
from queue_messaging.services import pubsub


logger = logging.getLogger(__name__)


class Envelope:
    def __init__(self, model, acknowledge_id, client):
        self.model = model
        self._acknowledge_id = acknowledge_id
        self._client = client

    def acknowledge(self):
        self._client.acknowledge(self._acknowledge_id)


class Messaging:
    def __init__(self, config: configuration.Configuration):
        self._client = pubsub.get_pubsub_client(config)
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
        if pulled_message is None:
            return
        model = self._get_model(pulled_message)
        return Envelope(
            model=model, acknowledge_id=pulled_message.ack_id,
            client=self._client)

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

    def _get_model(self, pulled_message: pubsub.PulledMessage):
        return encoding.decode_payload(
            header=encoding.create_header(pulled_message.attributes),
            encoded_data=pulled_message.data,
            message_config=self._types_to_model)
