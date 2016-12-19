import json
import uuid
import marshmallow
import pytest
from marshmallow import fields
from unittest import mock

from messaging import exceptions
from messaging.data import structures
from messaging.data import encoding


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
        data=data,
    )
    assert (json.loads(encoded_data.data)
            == json.loads('{"string_field": "123456789", "uuid_field": "72d9a041-f401-42b6-8556-72b3c00e43d8"}'))


def test_encode_invalid_data():
    with pytest.raises(exceptions.EncodingError):
        encoding.encode(
            data='invalid data',
        )


def test_payload_decoder_valid():
    header = mock.Mock(
        type='FancyEvent',
    )
    encoded_payload = '{"uuid_field": "72d9a041-f401-42b6-8556-72b3c00e43d8", "string_field": "123456789"}'
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
    encoded_payload = '{"uuid_field": "72d9a041-f401-42b6-8556-72b3c00e43d8", "string_field": "123456789"}'
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
    encoded_payload = 'invalid data'
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
    encoded_payload = '{}'
    message_config = {
        'FancyEvent': FancyEvent,
    }
    with pytest.raises(exceptions.DecodingError):
        encoding.decode_payload(
            header=header,
            encoded_data=encoded_payload,
            message_config=message_config,
        )
