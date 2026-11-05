"""Knowledge and message classes. They are part of the wire format.

A class's `name` is its key in the knowledge store. Instances are pickled, so
renaming or moving a class breaks every reader of data already stored.
"""


class TODOHistory(object):
    def __init__(self):  # no required arguments: read_knowledge calls TODOHistory()
        self.name = "TODOHistory"  # the knowledge key: unique, and equal to the class name
        self.uid = None  # stamped by rpclpy on write
        self.timestamp = None  # stamped by rpclpy on write
        self.samples = []  # plain public attributes: published classes are json.dumps(__dict__)


class TODOFinding(object):
    def __init__(self):
        self.name = "TODOFinding"
        self.uid = None
        self.timestamp = None
        self.value = None


class TODOPlan(object):
    def __init__(self):
        self.name = "TODOPlan"
        self.uid = None
        self.timestamp = None
        self.action = None
