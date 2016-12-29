from collections import namedtuple


QueueConfig = namedtuple(
    'Config',
    ['TOPIC', 'SUBSCRIPTION', 'DEAD_LETTER_TOPIC', 'PUBSUB_EMULATOR_HOST'],
)


class QueueConfigFactory:
    def __init__(self, config_dict):
        self.config_dict = config_dict

    def create(self) -> QueueConfig:
        return QueueConfig(
            self.config_dict['TOPIC'],
            self.config_dict['SUBSCRIPTION'],
            self.config_dict['DEAD_LETTER_TOPIC'],
            self.config_dict['PUBSUB_EMULATOR_HOST'],
        )
