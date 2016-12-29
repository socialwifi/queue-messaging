import collections


Header = collections.namedtuple('Header', ['type', 'datetime'])


class Model:
    @property
    def Meta(self):
        raise NotImplementedError

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __repr__(self):
        return '<{0}({1})>'.format(
            self.__class__.__name__,
            (', '.join(['='.join((key, repr(value))) for key, value in self.__dict__.items()])),
        )
