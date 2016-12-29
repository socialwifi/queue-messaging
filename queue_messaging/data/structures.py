import collections


Header = collections.namedtuple('Header', ['type', 'timestamp'])


class Model:
    @property
    def Meta(self):
        raise NotImplementedError

    def __init__(self, **kwargs):
        self._validate_with_schema_fields(kwargs, self.Meta.schema())
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return '<{0}({1})>'.format(
            self.__class__.__name__,
            (', '.join(['='.join((key, repr(value))) for key, value in self.__dict__.items()])),
        )

    @staticmethod
    def _validate_with_schema_fields(model_fields, schema):
        model_fields_names = set(model_fields.keys())
        schema_fields_names = set(schema.fields.keys())
        schema_required_fields_names = {
            field_name
            for field_name, field in schema.fields.items()
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
