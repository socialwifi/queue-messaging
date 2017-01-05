import json


class StringComparableJson:
    def __init__(self, json):
        self.json = json

    def __eq__(self, json_string):
        if isinstance(json_string, str):
            return self.json == json.loads(json_string)
        else:
            return self.json == json_string

    def __repr__(self):
        return repr(json.dumps(self.json))


class EncodedJson:
    def __init__(self, json, encoding='utf-8'):
        self.json = json
        self.encoding = encoding

    def __eq__(self, encoded):
        if isinstance(encoded, bytes):
            return self.json == json.loads(encoded.decode(self.encoding))
        else:
            return self.json == encoded

    def __repr__(self):
        return repr(json.dumps(self.json).encode())
