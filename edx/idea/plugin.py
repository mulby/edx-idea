
import logging

from stevedore import driver

from edx.idea.common.singleton import Singleton
from edx.idea.config import Configuration


log = logging.getLogger(__name__)


class PluginManager(object):
    __metaclass__ = Singleton

    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(process)d [%(name)s] %(filename)s:%(lineno)d - %(message)s'
        )
        log.info('Logging configured.')
        config = Configuration()
        mgr = driver.DriverManager(
            namespace='edx.idea.engine',
            name=config.get('engine', 'spark'),
            invoke_on_load=True,
        )
        self.engine = mgr.driver
