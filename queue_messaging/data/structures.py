import collections


Header = collections.namedtuple('Header', ['type', 'timestamp'])


class Model:
    @property
    def Meta(self):
        raise NotImplementedError

    def __init__(self, **kwargs):
        self._validate_with_schema_fields(kwargs, self._schema_fields)
        for field_name in self._schema_fields.keys():
            setattr(self, field_name, kwargs.get(field_name))

    def __repr__(self):
        return '<{0}({1})>'.format(
            self.__class__.__name__,
            (', '.join(['='.join((key, repr(value))) for key, value in self.__dict__.items()])),
        )

    def __eq__(self, other):
        return all(
            getattr(self, field_name, None) == getattr(other, field_name, None)
            for field_name in self._schema_fields.keys()
        )

    @property
    def _schema_fields(self):
        return self.Meta.schema().fields

    @staticmethod
    def _validate_with_schema_fields(model_fields, schema_fields):
        model_fields_names = set(model_fields.keys())
        schema_fields_names = set(schema_fields.keys())
        schema_required_fields_names = {
            field_name
            for field_name, field in schema_fields.items()
            if field.required
        }
        unexpected_fields = model_fields_names - schema_fields_names
        if len(unexpected_fields) > 0:
            raise TypeError("Got unexpected fields '{}'".format(
                unexpected_fields))
        missing_required_fields = schema_required_fields_names - model_fields_names
        if len(missing_required_fields) > 0:
            raise TypeError("Missing required fields '{}'".format(
                missing_required_fields))


PulledMessage = collections.namedtuple(
    'PulledMessage', ['ack', 'data', 'message_id', 'attributes'])
