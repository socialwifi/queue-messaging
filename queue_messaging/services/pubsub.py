import httplib2
from cached_property import cached_property
from google.cloud import pubsub
from google.gax import errors
from google.cloud import exceptions as google_cloud_exceptions

from queue_messaging import exceptions
from queue_messaging import utils


def get_pubsub_client(queue_config):
    return PubSub(
        topic_name=queue_config.TOPIC,
        subscription_name=queue_config.SUBSCRIPTION,
        pubsub_emulator_host=queue_config.PUBSUB_EMULATOR_HOST,
    )


def get_fallback_pubsub_client(queue_config):
    return PubSub(
        topic_name=queue_config.DEAD_LETTER_TOPIC,
        subscription_name=queue_config.SUBSCRIPTION,
        pubsub_emulator_host=queue_config.PUBSUB_EMULATOR_HOST,
    )


class PubSub:
    def __init__(self,
                 topic_name,
                 subscription_name=None,
                 pubsub_emulator_host=None):
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.pubsub_emulator_host = pubsub_emulator_host

    @cached_property
    def topic(self):
        return self.client.topic(self.topic_name)

    @cached_property
    def subscription(self):
        return self.topic.subscription(self.subscription_name)

    @cached_property
    def client(self):
        if self.pubsub_emulator_host:
            with utils.EnvironmentContext('PUBSUB_EMULATOR_HOST', self.pubsub_emulator_host):
                return pubsub.Client(http=httplib2.Http())
        else:
            return pubsub.Client()

    def send(self, data, **kwargs):
        try:
            return self.topic.publish(data, **kwargs)
        except (errors.GaxError, google_cloud_exceptions.NotFound) as e:
            raise exceptions.PubSubError('Error while sending a message.', error=e)

    def receive(self):
        try:
            result = self.subscription.pull(return_immediately=True)
            if result:
                return result.pop()
        except (errors.GaxError, google_cloud_exceptions.NotFound) as e:
            raise exceptions.PubSubError('Error while pulling a message.', errors=e)

    def acknowledge(self, msg_id):
        return self.subscription.acknowledge([msg_id])
