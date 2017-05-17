import httplib2
import tenacity
from cached_property import cached_property
from google.cloud import exceptions as google_cloud_exceptions
from google.cloud import pubsub
from google.gax import errors

from queue_messaging import exceptions
from queue_messaging import utils
from queue_messaging.data import structures


def get_pubsub_client(queue_config):
    return PubSub(
        topic_name=queue_config.TOPIC,
        subscription_name=queue_config.SUBSCRIPTION,
        pubsub_emulator_host=queue_config.PUBSUB_EMULATOR_HOST,
        use_grpc=queue_config.USE_GRPC,
    )


def get_fallback_pubsub_client(queue_config):
    return PubSub(
        topic_name=queue_config.DEAD_LETTER_TOPIC,
        subscription_name=queue_config.SUBSCRIPTION,
        pubsub_emulator_host=queue_config.PUBSUB_EMULATOR_HOST,
        use_grpc=queue_config.USE_GRPC,
    )


retry = tenacity.retry(
    retry=tenacity.retry_if_exception_type(
        (errors.GaxError, ConnectionError)
    ),
    stop=tenacity.stop_after_attempt(max_attempt_number=3),
    reraise=True,
)


class PubSub:
    def __init__(self,
                 topic_name,
                 subscription_name=None,
                 pubsub_emulator_host=None,
                 use_grpc=False):
        self.topic_name = topic_name
        self.subscription_name = subscription_name
        self.pubsub_emulator_host = pubsub_emulator_host
        self.use_grpc = use_grpc

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
                return pubsub.Client(_http=httplib2.Http(), _use_grpc=self.use_grpc)
        else:
            return pubsub.Client(_use_grpc=self.use_grpc)

    @retry
    def send(self, message: str, **attributes):
        bytes_payload = message.encode('utf-8')
        try:
            return self.topic.publish(bytes_payload, **attributes)
        except google_cloud_exceptions.NotFound as e:
            raise exceptions.PubSubError('Error while sending a message.', error=e)

    @retry
    def receive(self) -> structures.PulledMessage:
        try:
            result = self.subscription.pull(max_messages=1, return_immediately=True)
            if result:
                ack_id, message = result.pop()
                return structures.PulledMessage(
                    ack_id=ack_id, data=message.data.decode('utf-8'),
                    message_id=message.message_id, attributes=message.attributes)
        except google_cloud_exceptions.NotFound as e:
            raise exceptions.PubSubError('Error while pulling a message.', errors=e)

    @retry
    def acknowledge(self, msg_id):
        return self.subscription.acknowledge([msg_id])
