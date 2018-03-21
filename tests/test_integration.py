import datetime
import uuid
from unittest import mock

import marshmallow
import pytest
from marshmallow import fields

import queue_messaging
from queue_messaging import test_utils


class FancyEventSchema(marshmallow.Schema):
    uuid_field = fields.UUID(required=True)
    string_field = fields.String(required=False)


class FancyEvent(queue_messaging.Model):
    class Meta:
        schema = FancyEventSchema
        type_name = 'FancyEvent'


@pytest.fixture()
def pubsub_client_mock():
    with mock.patch('google.cloud.pubsub.SubscriberClient') as client:
        yield client


@pytest.fixture()
def pubsub_publisher_client_mock():
    with mock.patch('google.cloud.pubsub.PublisherClient') as client:
        yield client


@pytest.fixture()
def topic_path_mock():
    with mock.patch('queue_messaging.services.pubsub.PubSub._get_topic_path') as path:
        yield path


@pytest.fixture()
def header_timestamp_mock():
    with mock.patch('queue_messaging.data.encoding.get_now_with_utc_timezone') as now_mock:
        yield now_mock


def test_send(header_timestamp_mock, pubsub_publisher_client_mock, topic_path_mock):
    topic_path_mock.return_value = 'projects/p-id/topics/test-topic'
    messaging = queue_messaging.Messaging.create_from_dict({
        'TOPIC': 'test-topic',
        'PROJECT_ID': 'p-id',
    })
    model = FancyEvent(
        uuid_field=uuid.UUID('cd1d3a03-7b04-4a35-97f8-ee5f3eb04c8e'),
        string_field='Just testing!'
    )
    header_timestamp_mock.return_value = datetime.datetime(
        2016, 12, 10, 11, 15, 45, 123456, tzinfo=datetime.timezone.utc)

    messaging.send(model)

    publish_mock = pubsub_publisher_client_mock.return_value.publish
    publish_mock.topic_path.return_value = 'z'
    publish_mock.assert_called_with(
        'projects/p-id/topics/test-topic',
        test_utils.EncodedJson({
            "uuid_field": "cd1d3a03-7b04-4a35-97f8-ee5f3eb04c8e",
            "string_field": "Just testing!"
        }),
        timestamp='2016-12-10T11:15:45.123456Z',
        type='FancyEvent'
    )


def test_receive(pubsub_client_mock):
    messaging = queue_messaging.Messaging.create_from_dict({
        'SUBSCRIPTION': 'test-subscription',
        'MESSAGE_TYPES': [
            FancyEvent,
        ],
        'PROJECT_ID': 'p-id',
    })
    subscription_mock = pubsub_client_mock.return_value.subscribe.return_value

    messaging.receive(callback=mock.MagicMock())
    assert subscription_mock.open.called
