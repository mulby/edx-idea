
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

    def __init__(self, steps=None, name=None):
        self.steps = steps or []
        self.name = name or ('phase_' + generate_uuid())

    def __repr__(self):
        return 'Phase(steps={0}, name={1})'.format(
            repr(self.steps),
            repr(self.name)
        )

    def __str__(self):
        return 'Phase[{}]'.format(self.name)

    @staticmethod
    def from_struct(struct):
        steps = [Step.from_struct(p) for p in struct['steps']]
        return Phase(steps=steps, name=struct.get('name'))


class Step(object):

    def __init__(self, path, args=None, name=None):
        self.path = path
        self.args = args or []
        self.name = name or ('step_' + generate_uuid())

    def __repr__(self):
        return 'Step(path={0}, args={1}, name={2})'.format(
            repr(self.path),
            repr(self.args),
            repr(self.name)
        )

    def __str__(self):
        return 'Step[{0}:{1}]'.format(self.name, self.path)

    @staticmethod
    def from_struct(struct):
        return Step(path=struct['path'], args=struct.get('args'), name=struct.get('name'))
