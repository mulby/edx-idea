
from edx.idea.common.identifier import generate_uuid


class Workflow(object):

    def __init__(self, phases=None, name=None):
        self.phases = phases or []
        self.name = name or ('workflow_' + generate_uuid())

    def __repr__(self):
        return 'Workflow(phases={0}, name={1})'.format(
            repr(self.phases),
            repr(self.name)
        )

    def __str__(self):
        return 'Workflow[{}]'.format(self.name)

    @staticmethod
    def from_struct(struct):
        phases = [Phase.from_struct(p) for p in struct['phases']]
        return Workflow(phases=phases, name=struct.get('name'))


class Phase(object):

    def __init__(self, tasks=None, name=None):
        self.tasks = tasks or []
        self.name = name or ('phase_' + generate_uuid())

    def __repr__(self):
        return 'Phase(tasks={0}, name={1})'.format(
            repr(self.tasks),
            repr(self.name)
        )

    def __str__(self):
        return 'Phase[{}]'.format(self.name)

    @staticmethod
    def from_struct(struct):
        tasks = [Task.from_struct(p) for p in struct['tasks']]
        return Phase(tasks=tasks, name=struct.get('name'))


class Task(object):

    def __init__(self, path, args=None, name=None):
        self.path = path
        self.args = args or []
        self.name = name or ('task_' + generate_uuid())

    def __repr__(self):
        return 'Task(path={0}, args={1}, name={2})'.format(
            repr(self.path),
            repr(self.args),
            repr(self.name)
        )

    def __str__(self):
        return 'Task[{0}:{1}]'.format(self.name, self.path)

    @staticmethod
    def from_struct(struct):
        return Task(path=struct['path'], args=struct.get('args'), name=struct.get('name'))
