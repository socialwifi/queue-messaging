import json
import uuid
import marshmallow
import pytest
from marshmallow import fields
from unittest import mock
import datetime

from queue_messaging import exceptions
from queue_messaging.data import structures
from queue_messaging.data import encoding


class FancyEventSchema(marshmallow.Schema):
    string_field = fields.String(required=True)
    uuid_field = fields.UUID(required=True)


class FancyEvent(structures.Model):
    class Meta:
        schema = FancyEventSchema


def test_encoding_payload_valid():
    data = FancyEvent(
        string_field='123456789',
        uuid_field=uuid.UUID('72d9a041-f401-42b6-8556-72b3c00e43d8'),
    )
    encoded_data = encoding.encode(
        model=data,
    )
    assert (json.loads(encoded_data)
            == json.loads('{"string_field": "123456789", "uuid_field": "72d9a041-f401-42b6-8556-72b3c00e43d8"}'))


def test_encode_invalid_data():
    with pytest.raises(exceptions.EncodingError):
        encoding.encode(
            model='invalid data',
        )


def test_payload_decoder_valid():
    header = mock.Mock(
        type='FancyEvent',
    )
    encoded_payload = b'{"uuid_field": "72d9a041-f401-42b6-8556-72b3c00e43d8", "string_field": "123456789"}'
    message_config = {
        'FancyEvent': FancyEvent,
    }
    encoding.decode_payload(
        header=header,
        encoded_data=encoded_payload,
        message_config=message_config,
    )


def test_payload_decoder_invalid_header():
    header = mock.Mock(
        type='NonExistingEvent'
    )
    encoded_payload = b'{"uuid_field": "72d9a041-f401-42b6-8556-72b3c00e43d8", "string_field": "123456789"}'
    message_config = {
        'FancyEvent': FancyEvent,
    }
    with pytest.raises(exceptions.DecodingError):
        encoding.decode_payload(
            header=header,
            encoded_data=encoded_payload,
            message_config=message_config,
        )


def test_payload_decoder_invalid_data():
    header = mock.Mock(
        type='FancyEvent',
    )
    encoded_payload = b'invalid data'
    message_config = {
        'FancyEvent': FancyEvent,
    }
    with pytest.raises(exceptions.DecodingError):
        encoding.decode_payload(
            header=header,
            encoded_data=encoded_payload,
            message_config=message_config,
        )


def test_payload_decoder_empty_data():
    header = mock.Mock(
        type='FancyEvent',
    )
    encoded_payload = b'{}'
    message_config = {
        'FancyEvent': FancyEvent,
    }
    with pytest.raises(exceptions.DecodingError):
        encoding.decode_payload(
            header=header,
            encoded_data=encoded_payload,
            message_config=message_config,
        )


class TestDatetimeRfc3339Encoding:
    def test_integration(self):
        input = datetime.datetime(2016, 12, 10, 11, 15, 45, 123456,
                                  tzinfo=datetime.timezone.utc)
        encoded = encoding.datetime_to_rfc3339_string(input)
        decoded = encoding.rfc3339_string_to_datetime(encoded)
        assert decoded == input

    def test_encoding_naive_datetime(self):
        input = datetime.datetime(2016, 12, 10, 11, 15, 45, 123456)
        encoded = encoding.datetime_to_rfc3339_string(input)
        assert encoded == '2016-12-10T11:15:45.123456Z'

    def test_encoding_datetime_in_utc(self):
        input = datetime.datetime(2016, 12, 10, 11, 15, 45, 123456,
                                  tzinfo=datetime.timezone.utc)
        encoded = encoding.datetime_to_rfc3339_string(input)
        assert encoded == '2016-12-10T11:15:45.123456Z'

    def test_encoding_datetime_not_in_utc(self):
        some_timezone = datetime.timezone(datetime.timedelta(hours=-7))
        input = datetime.datetime(2016, 12, 10, 11, 15, 45, 123456,
                                  tzinfo=some_timezone)
        encoded = encoding.datetime_to_rfc3339_string(input)
        assert encoded == '2016-12-10T18:15:45.123456Z'

    def test_decoding_valid_string(self):
        input = '2016-12-10T11:15:45.123456Z'
        decoded = encoding.rfc3339_string_to_datetime(input)
        assert decoded == datetime.datetime(2016, 12, 10, 11, 15, 45, 123456,
                                            tzinfo=datetime.timezone.utc)

    def test_decoding_invalid_string(self):
        input = '2016-12-10T11:15:45.123456+00:00'
        with pytest.raises(ValueError) as excinfo:
            encoding.rfc3339_string_to_datetime(input)
        assert str(excinfo.value) == (
            "time data '2016-12-10T11:15:45.123456+00:00' "
            "does not match format '%Y-%m-%dT%H:%M:%S.%fZ'")
