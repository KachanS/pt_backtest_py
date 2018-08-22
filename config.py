import os
import json
import configparser

from services.decorators.singleton import SingletonDecorator

basedir = os.path.abspath(os.path.dirname(__file__))

@SingletonDecorator
class FileConfig:

    def __init__(self):
        self.config = configparser.ConfigParser()
        with open(os.path.join(basedir, 'config.json'), 'r') as f:
            self.config = json.load(f)

    def get(self, key, default=None, parser=None):
        section, option = key.split('.')
        if section not in self.config:
            return default
        if option not in self.config[section]:
            return default
        if parser is None:
            return self.config[section][option]
        if parser is bool:
            return bool(int(self.config[section][option]))
        return parser(self.config[section][option])
