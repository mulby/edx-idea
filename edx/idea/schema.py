from collections import namedtuple
from collections import OrderedDict


Field = namedtuple('Field', ['name', 'data_type'])


class Schema(object):

    def __init__(self, fields, primary_key=None):
        self.fields = OrderedDict()
        for field in fields:
            self.fields[field.name] = field
        self.primary_key = self.fields[primary_key] if primary_key else None

    def fields_without_key(self):
        for _, field in self.fields.iteritems():
            if not self.primary_key or field.name != self.primary_key.name:
                yield field

    def __repr__(self):
        return 'Schema(fields={0}, primary_key={1})'.format(
            repr([f for _, f in self.fields.items()]),
            repr(None) if not self.primary_key else repr(self.primary_key.name)
        )
