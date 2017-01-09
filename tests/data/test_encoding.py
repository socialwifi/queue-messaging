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
        type_name = 'FancyEvent'


class EmptySchema(marshmallow.Schema):
    pass


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


def test_if_encode_raises_exception_with_invalid_data_and_strict_schema():
    class StrictSchema(marshmallow.Schema):
        uuid_field = fields.UUID(required=True)

        class Meta:
            strict = True

    class Event(structures.Model):
        class Meta:
            schema = StrictSchema
            type_name = 'Event'

    data = Event(uuid_field='not an uuid')
    with pytest.raises(exceptions.EncodingError) as excinfo:
        encoding.encode(data)
    assert str(excinfo.value) == (
        "({'uuid_field': ['Not a valid UUID.']}, '')")


def test_if_encode_raises_exception_with_invalid_data_and_not_strict_schema():
    class NotStrictSchema(marshmallow.Schema):
        uuid_field = fields.UUID(required=True)

    class Event(structures.Model):
        class Meta:
            schema = NotStrictSchema
            type_name = 'Event'

    data = Event(uuid_field='not an uuid')
    with pytest.raises(exceptions.EncodingError) as excinfo:
        encoding.encode(data)
    assert str(excinfo.value) == (
        "({'uuid_field': ['Not a valid UUID.']}, '')")


def test_encode_invalid_data():
    with pytest.raises(exceptions.EncodingError):
        encoding.encode(
            model='invalid data',
        )


def test_create_attributes():
    data = FancyEvent(
        string_field='123456789',
        uuid_field=uuid.UUID('72d9a041-f401-42b6-8556-72b3c00e43d8'),
    )
    now = datetime.datetime(2016, 12, 10, 11, 15, 45, tzinfo=datetime.timezone.utc)
    attributes = encoding.create_attributes(data, now=now)
    assert attributes == {
        'type': 'FancyEvent',
        'timestamp': '2016-12-10T11:15:45.000000Z',
    }


@mock.patch('queue_messaging.data.encoding.get_now_with_utc_timezone')
def test_create_attributes_uses_current_date_for_timestamp(get_now_with_utc_timezone):
    data = FancyEvent(
        string_field='123456789',
        uuid_field=uuid.UUID('72d9a041-f401-42b6-8556-72b3c00e43d8'),
    )
    get_now_with_utc_timezone.return_value = datetime.datetime(
        2016, 12, 10, 11, 15, 45, tzinfo=datetime.timezone.utc)
    attributes = encoding.create_attributes(data)
    assert attributes == {
        'type': 'FancyEvent',
        'timestamp': '2016-12-10T11:15:45.000000Z',
    }


def test_create_attributes_raises_error_when_no_type_present():
    class BadlyDefinedEvent(structures.Model):
        class Meta:
            schema = EmptySchema

    data = BadlyDefinedEvent()
    with pytest.raises(exceptions.ConfigurationError) as excinfo:
        encoding.create_attributes(data)

    assert (str(excinfo.value) ==
            "Missing Meta.type_name declaration in model: <BadlyDefinedEvent()>")


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


class TestDecode:
    @pytest.fixture
    def schema_class(self):
        class Schema(marshmallow.Schema):
            uuid_field = fields.UUID(required=True)
            string_field = fields.String(required=False)
        return Schema

    @pytest.fixture
    def event_class(self, schema_class):
        class Event(structures.Model):
            class Meta:
                schema = schema_class
                type_name = 'Event'
        return Event

    def test_if_works(self, event_class):
        data = ('{"uuid_field": "72d9a041-f401-42b6-8556-72b3c00e43d8", '
                '"string_field": "123456789"}')
        result = encoding.decode(type=event_class, encoded_data=data)
        assert result == event_class(
            uuid_field=uuid.UUID('72d9a041-f401-42b6-8556-72b3c00e43d8'),
            string_field='123456789'
        )

    def test_if_works_when_optional_field_is_missing(self, event_class):
        data = '{"uuid_field": "72d9a041-f401-42b6-8556-72b3c00e43d8"}'
        result = encoding.decode(type=event_class, encoded_data=data)
        assert result == event_class(
            uuid_field=uuid.UUID('72d9a041-f401-42b6-8556-72b3c00e43d8')
        )

    def test_if_works_when_additional_field_is_present(self, event_class):
        data = ('{"uuid_field": "72d9a041-f401-42b6-8556-72b3c00e43d8", '
                '"string_field": "123456789", '
                '"new_field": "does it work?"}')
        result = encoding.decode(type=event_class, encoded_data=data)
        assert result == event_class(
            uuid_field=uuid.UUID('72d9a041-f401-42b6-8556-72b3c00e43d8'),
            string_field='123456789'
        )

    def test_if_raises_exception_with_invalid_data_and_strict_schema(self):
        class StrictSchema(marshmallow.Schema):
            uuid_field = fields.UUID(required=True)

            class Meta:
                strict = True

        class Event(structures.Model):
            class Meta:
                schema = StrictSchema
                type_name = 'Event'

        data = '{"uuid_field": "not an uuid"}'
        with pytest.raises(exceptions.DecodingError) as excinfo:
            encoding.decode(type=Event, encoded_data=data)
        assert str(excinfo.value) == (
            "({'uuid_field': ['Not a valid UUID.']}, '')")

    def test_if_raises_exception_with_invalid_data_and_not_strict_schema(self):
        class NotStrictSchema(marshmallow.Schema):
            uuid_field = fields.UUID(required=True)

        class Event(structures.Model):
            class Meta:
                schema = NotStrictSchema
                type_name = 'Event'

        data = '{"uuid_field": "not an uuid"}'
        with pytest.raises(exceptions.DecodingError) as excinfo:
            encoding.decode(type=Event, encoded_data=data)
        assert str(excinfo.value) == (
            "({'uuid_field': ['Not a valid UUID.']}, '')")


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
