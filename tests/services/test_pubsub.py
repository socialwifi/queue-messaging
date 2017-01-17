from unittest import mock

import pytest
from google.gax import errors as gax_errors

from queue_messaging.services import pubsub


class TestPubSub:
    def test_receive(self, pull_mock):
        valid_response = self.valid_response_factory(message_id=1)
        pull_mock.return_value = valid_response
        client = pubsub.PubSub(topic_name=mock.Mock())
        message = client.receive()
        assert message.message_id == 1

    def test_retrying_receive(self, pull_mock):
        valid_response = self.valid_response_factory(message_id=1)
        pull_mock.side_effect = [
            ConnectionResetError, valid_response
        ]
        client = pubsub.PubSub(topic_name=mock.Mock())
        message = client.receive()
        assert message.message_id == 1

    def test_send(self, publish_mock):
        publish_mock.return_value = '123'
        client = pubsub.PubSub(topic_name=mock.Mock())
        result = client.send(message='')
        publish_mock.assert_called_with(b'')
        assert result == '123'

    def test_retrying_send(self, publish_mock):
        publish_mock.side_effect = [
            ConnectionResetError, '123'
        ]
        client = pubsub.PubSub(topic_name=mock.Mock())
        result = client.send(message='')
        assert result == '123'

    def test_acknowledge(self, acknowledge_mock):
        client = pubsub.PubSub(topic_name=mock.Mock())
        client.acknowledge(msg_id='123')
        acknowledge_mock.assert_called_with(['123'])

    def test_retrying_acknowledge(self, acknowledge_mock):
        acknowledge_mock.side_effect = [
            ConnectionResetError, None
        ]
        client = pubsub.PubSub(topic_name=mock.Mock())
        client.acknowledge(msg_id='123')
        assert acknowledge_mock.call_count == 2

    @staticmethod
    def valid_response_factory(*, message_id=1):
        message = mock.MagicMock(
            data=(b'{"uuid_field": "cd1d3a03-7b04-4a35-97f8-ee5f3eb04c8e", '
                  b'"string_field": "Just testing!"}'),
            message_id=message_id,
            attributes={
                'timestamp': '2016-12-10T11:15:45.123456Z',
                'type': 'FancyEvent',
            }
        )
        return [(123, message)]


@pytest.fixture
def pull_mock(subscription_mock):
    return subscription_mock.pull


@pytest.fixture
def publish_mock(topic_mock):
    return topic_mock.publish


@pytest.fixture
def acknowledge_mock(subscription_mock):
    return subscription_mock.acknowledge


@pytest.fixture
def subscription_mock(topic_mock):
    return topic_mock.subscription.return_value


@pytest.fixture
def topic_mock(pubsub_client_mock):
    return pubsub_client_mock.return_value.topic.return_value


@pytest.fixture
def pubsub_client_mock():
    with mock.patch('google.cloud.pubsub.Client') as client:
        yield client


class TestRetry:
    @pytest.fixture()
    def mocked_function(self):
        return mock.MagicMock(_is_coroutine=False)

    def test_when_works(self, mocked_function):
        mocked_function.return_value = 1
        decorated = pubsub.retry(mocked_function)
        result = decorated()
        assert result == 1

    def test_retrying_on_connection_error(self, mocked_function):
        mocked_function.side_effect = [ConnectionResetError, 1]
        decorated = pubsub.retry(mocked_function)
        result = decorated()
        assert result == 1

    def test_retrying_on_gax_error(self, mocked_function):
        mocked_function.side_effect = [
            gax_errors.GaxError(msg="RPC failed"), 1
        ]
        decorated = pubsub.retry(mocked_function)
        result = decorated()
        assert result == 1

    def test_if_failed_retry_reraises(self, mocked_function):
        mocked_function.side_effect = [
            ConnectionResetError, BrokenPipeError, ConnectionRefusedError, 1
        ]
        decorated = pubsub.retry(mocked_function)
        pytest.raises(ConnectionRefusedError, decorated)
