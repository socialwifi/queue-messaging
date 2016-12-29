import json

from queue_messaging import exceptions
from queue_messaging.data import structures


def encode(data: structures.Model):
    try:
        return data.Meta.schema().dumps(data)
    except AttributeError as e:
        raise exceptions.EncodingError(e)


def decode_payload(header: structures.Header, encoded_data: str, message_config: dict):
    try:
        type = message_config[header.type]
    except AttributeError:
        raise exceptions.DecodingError('Invalid header.', header=header)
    except KeyError:
        raise exceptions.DecodingError('Unknown type.', header_type=header.type)
    else:
        return decode(type, encoded_data)


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
