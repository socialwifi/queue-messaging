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
    with mock.patch('google.cloud.pubsub.Client') as client:
        yield client


@pytest.fixture()
def header_timestamp_mock():
    with mock.patch('queue_messaging.data.encoding.get_now_with_utc_timezone') as now_mock:
        yield now_mock


def test_send(header_timestamp_mock, pubsub_client_mock):
    messaging = queue_messaging.Messaging.create_from_dict({
        'TOPIC': 'test-topic',
    })
    model = FancyEvent(
        uuid_field=uuid.UUID('cd1d3a03-7b04-4a35-97f8-ee5f3eb04c8e'),
        string_field='Just testing!'
    )
    header_timestamp_mock.return_value = datetime.datetime(
        2016, 12, 10, 11, 15, 45, 123456, tzinfo=datetime.timezone.utc)

    messaging.send(model)

    topic_mock = pubsub_client_mock.return_value.topic
    publish_mock = topic_mock.return_value.publish
    topic_mock.assert_called_with('test-topic')
    publish_mock.assert_called_with(
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
    })
    topic_mock = pubsub_client_mock.return_value.topic.return_value
    subscription_mock = topic_mock.subscription.return_value
    mocked_message = mock.MagicMock(
        data=(b'{"uuid_field": "cd1d3a03-7b04-4a35-97f8-ee5f3eb04c8e", '
              b'"string_field": "Just testing!"}'),
        message_id=1,
        attributes={
            'timestamp': '2016-12-10T11:15:45.123456Z',
            'type': 'FancyEvent',
        }
    )
    subscription_mock.pull.return_value = [
        (123, mocked_message)
    ]

    envelope = messaging.receive()

    topic_mock.subscription.assert_called_with('test-subscription')
    assert envelope.model == FancyEvent(
        uuid_field=uuid.UUID('cd1d3a03-7b04-4a35-97f8-ee5f3eb04c8e'),
        string_field='Just testing!'
    )

    envelope.acknowledge()

    subscription_mock.acknowledge.assert_called_with([123])
