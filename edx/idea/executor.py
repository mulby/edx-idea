
import logging
from multiprocessing import Pool
import os
import sys

from jinja2 import Environment, FileSystemLoader
from jinja2.exceptions import TemplateNotFound
import yaml

from edx.idea.plugin import PluginManager
from edx.idea.workflow import Workflow


log = logging.getLogger(__name__)


class Executor(object):

    def __init__(self):
        self.engine = PluginManager().engine
        self.pool = Pool()

    def execute(self, workflow):
        log.info('Executing %s.', str(workflow))
        for phase in workflow.phases:
            log.info('Executing %s.', str(phase))
            self.pool.map(run_task, phase.tasks)
            log.info('%s complete.', str(phase))
        log.info('%s complete.', str(workflow))


def run_task(task):
    engine = PluginManager().engine
    log.info('Executing %s.', str(task))
    engine.run(task)
    log.info('%s complete.', str(task))


def main():
    executor = Executor()

    workflow_yaml_path = sys.argv[1]
    log.info('Loading workflow from yaml file %s.', workflow_yaml_path)

    template_env = Environment(loader=FileSystemLoader(os.getcwd()))
    try:
        template = template_env.get_template(workflow_yaml_path)
    except TemplateNotFound:
        return
    workflow_yaml = template.render(env=os.environ, argv=sys.argv[2:])

    workflow_struct = yaml.load(workflow_yaml)
    log.debug('Parsed workflow struct = %s.', str(workflow_struct))
    workflow = Workflow.from_struct(workflow_struct)
    log.debug('Parsed workflow = %s.', str(workflow))
    executor.execute(workflow)
