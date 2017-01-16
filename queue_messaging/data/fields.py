from marshmallow import fields
import netaddr


class MACAddressField(fields.Field):
    default_error_messages = {
        'invalid': 'Not a valid MAC.',
        'format': '"{input}" cannot be formatted as MAC.',
    }
    default_dialect = netaddr.mac_unix_expanded

    def _serialize(self, value, attr, obj):
        if value is None:
            return None
        try:
            return str(self._to_python(value))
        except netaddr.AddrFormatError:
            self.fail('format', input=value)

    def _deserialize(self, value, attr, data):
        if not value:
            raise self.fail('invalid')
        try:
            return self._to_python(value)
        except netaddr.AddrFormatError:
            self.fail('format', input=value)

    def _to_python(self, value):
        eui = netaddr.EUI(value)
        eui.dialect = self.default_dialect
        return eui
