import marshmallow
import pytest
from marshmallow import fields

from queue_messaging.data import structures


class FancyModelSchema(marshmallow.Schema):
    uuid_field = fields.UUID(required=True)
    string_field = fields.String(required=False)


class FancyModel(structures.Model):
    class Meta:
        schema = FancyModelSchema


def test_creating_model():
    model = FancyModel(
        uuid_field=1,
        string_field='123456789',
    )
    assert model.uuid_field == 1
    assert model.string_field == '123456789'


def test_if_creating_model_with_missing_optional_field_is_ok():
    model = FancyModel(
        uuid_field=1,
    )
    assert model.uuid_field == 1
    assert model.string_field is None


def test_if_creating_model_with_invalid_fields_raises_exception():
    with pytest.raises(TypeError) as excinfo:
        FancyModel(
            uuid_field=1,
            string_field='123456789',
            not_existing_field='abc'
        )
    assert str(excinfo.value) == "Got unexpected fields '{'not_existing_field'}'"


def test_if_creating_model_with_missing_required_field_raises_exception():
    with pytest.raises(TypeError) as excinfo:
        FancyModel(
            string_field='aaa',
        )
    assert str(excinfo.value) == "Missing required fields '{'uuid_field'}'"
