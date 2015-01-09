
import logging.config
import os

from jinja2 import Environment, FileSystemLoader
from jinja2.exceptions import TemplateNotFound
import yaml
from yaml.parser import ParserError

from edx.idea.common.wrapper import MapWrapper


class Configuration(MapWrapper):

    def __init__(self):
        self.config = self.load()
        super(Configuration, self).__init__(self.config)

        logging_config = self.config.get('logging')
        if logging_config:
            logging.config.dictConfig(logging_config)

    def load(self):
        template_env = Environment(loader=FileSystemLoader(os.getcwd()))
        try:
            template = template_env.get_template('config.yml')
        except TemplateNotFound:
            return {}
        config_yaml = template.render(env=os.environ)
        try:
            return yaml.load(config_yaml)
        except (IOError, ParserError):
            return {}

    def get_nested(self, *keys, **kwargs):
        cur = self
        for key in keys:
            try:
                cur = cur.get(key)
            except AttributeError:
                break

        if cur is None:
            if 'default' in kwargs:
                return kwargs['default']
            else:
                raise ValueError('Could not find configuration setting {}'.format('.'.join(keys)))

        return cur

    def get_env(self, *keys, **kwargs):
        if 'env_var' in kwargs:
            env_var = kwargs.pop('env_var')
            if env_var in os.environ:
                return os.environ[env_var]

        return self.get_nested(*keys, **kwargs)
