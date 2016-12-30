import datetime
import json

from queue_messaging import exceptions
from queue_messaging.data import structures


def encode(model: structures.Model):
    try:
        return model.Meta.schema().dumps(model).data
    except AttributeError as e:
        raise exceptions.EncodingError(e)


def decode_payload(header: structures.Header, encoded_data: bytes, message_config: dict):
    try:
        type = message_config[header.type]
    except AttributeError:
        raise exceptions.DecodingError('Invalid header.', header=header)
    except KeyError:
        raise exceptions.DecodingError('Unknown type.', header_type=header.type)
    else:
        return decode(type, encoded_data.decode('utf-8'))


def decode(type, encoded_data: str):
    try:
        decoded_data = type.Meta.schema().loads(encoded_data)
    except (json.decoder.JSONDecodeError, TypeError, AttributeError):
        raise exceptions.DecodingError('Error while decoding.', encoded_data=encoded_data)
    else:
        if decoded_data.errors:
            raise exceptions.DecodingError(decoded_data.errors)
        else:
            return type(**decoded_data.data)


def create_attributes(model: structures.Model, mapping, now=None) -> dict:
    if now is None:
        now = get_now_with_utc_timezone()
    try:
        type_name = mapping[model.__class__]
    except KeyError:
        raise exceptions.ConfigurationError(
            'No type defined for model: {}'.format(model))
    else:
        return {
            'type': type_name,
            'timestamp': datetime_to_rfc3339_string(now),
        }


def get_now_with_utc_timezone() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def create_header(attributes):
    try:
        type = attributes['type']
        timestamp = attributes['timestamp']
    except KeyError:
        raise exceptions.DecodingError(
            'Missing attributes in message.', attributes=attributes)
    try:
        timestamp = rfc3339_string_to_datetime(timestamp)
    except ValueError:
        raise exceptions.DecodingError(
            'Timestamp in header is not in a valid RFC3339 format.',
            timestamp=timestamp)
    return structures.Header(
        type=type,
        timestamp=timestamp
    )


RFC3339_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'


def datetime_to_rfc3339_string(value: datetime.datetime):
    if value.tzinfo is not None:
        value = value.replace(tzinfo=None) - value.utcoffset()
    return value.strftime(RFC3339_FORMAT)


def rfc3339_string_to_datetime(value):
    return datetime.datetime.strptime(
        value, RFC3339_FORMAT).replace(tzinfo=datetime.timezone.utc)
