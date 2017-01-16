import types

import marshmallow
import netaddr
import pytest

from queue_messaging.data import fields


class TestMACAddressField:
    @pytest.fixture
    def model(self):
        return types.SimpleNamespace()

    def test_integration(self, model):
        field = fields.MACAddressField()
        model.mac = '78-F8-82-B2-E5-5A'
        serialized = field.serialize('mac', model)
        deserialized = field.deserialize(serialized)
        assert deserialized == '78:f8:82:b2:e5:5a'

    def test_serialization_with_unix_dialect(self, model):
        field = fields.MACAddressField()
        model.mac = '78:f8:82:b2:e5:5a'
        serialized = field.serialize('mac', model)
        assert serialized == '78:f8:82:b2:e5:5a'

    def test_serialization_with_unix_dialect_uppercase(self, model):
        field = fields.MACAddressField()
        model.mac = '78:F8:82:B2:E5:5A'
        serialized = field.serialize('mac', model)
        assert serialized == '78:f8:82:b2:e5:5a'

    def test_serialization_with_eui48_dialect(self, model):
        field = fields.MACAddressField()
        model.mac = '78-F8-82-B2-E5-5A'
        serialized = field.serialize('mac', model)
        assert serialized == '78:f8:82:b2:e5:5a'

    def test_serialization_with_eui_in_different_dialect(self, model):
        field = fields.MACAddressField()
        model.mac = netaddr.EUI('78-F8-82-B2-E5-5A', dialect=netaddr.mac_eui48)
        serialized = field.serialize('mac', model)
        assert serialized == '78:f8:82:b2:e5:5a'

    def test_serialization_with_empty_mac(self, model):
        field = fields.MACAddressField()
        model.mac = None
        serialized = field.serialize('mac', model)
        assert serialized is None

    def test_serialization_with_invalid_data(self, model):
        field = fields.MACAddressField()
        model.mac = 'random string'
        with pytest.raises(marshmallow.ValidationError) as e:
            field.serialize('mac', model)
        assert str(e.value) == "\"random string\" cannot be formatted as MAC."

    def test_deserialization(self):
        field = fields.MACAddressField()
        value = '78:f8:82:b2:e5:5a'
        deserialized = field.deserialize(value)
        assert deserialized == '78:f8:82:b2:e5:5a'
        assert repr(deserialized) == 'EUI(\'78:f8:82:b2:e5:5a\')'
        assert deserialized.dialect == netaddr.mac_unix_expanded

    def test_deserialization_with_eui48_dialect(self):
        field = fields.MACAddressField()
        value = '78-F8-82-B2-E5-5A'
        deserialized = field.deserialize(value)
        assert deserialized == '78:f8:82:b2:e5:5a'
        assert deserialized.dialect == netaddr.mac_unix_expanded

    def test_deserialization_with_invalid_data(self):
        field = fields.MACAddressField()
        value = 'random string'
        with pytest.raises(marshmallow.ValidationError) as e:
            field.deserialize(value)
        assert str(e.value) == "\"random string\" cannot be formatted as MAC."

    def test_deserialization_with_empty_data(self):
        field = fields.MACAddressField()
        value = None
        with pytest.raises(marshmallow.ValidationError) as e:
            field.deserialize(value)
        assert str(e.value) == "Field may not be null."
