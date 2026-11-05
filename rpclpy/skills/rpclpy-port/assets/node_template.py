"""Skeleton for a ported MAPLE-K loop. Copy it, then replace every TODO.

Keep user code between the cc_ markers: a regeneration from the component model
preserves what is inside them. Keep each `# LINK` comment equal to its topic.
"""
import json
import threading

import yaml

from rpclpy.node import Node
from rpclpy.utils import timeit_callback
from TODO_messages import *  # knowledge classes live in their own module so they stay picklable


class Monitor(Node):

    def __init__(self, config, verbose=True, **kw):
        super().__init__(config=config, verbose=verbose, **kw)
        self._name = "Monitor"
        self.logger.info("Monitor instantiated")

        #<!-- cc_init START--!>
        # Seed every knowledge class here: a read before the first write returns None.
        self.write_knowledge(TODOHistory())
        self.write_knowledge(TODOFinding())
        self.write_knowledge(TODOPlan())
        #<!-- cc_init END--!>

    @timeit_callback
    def monitor(self, msg):
        #<!-- cc_code_monitor START--!>
        reading = json.loads(msg)  # the payload is always a JSON string
        _history = self.read_knowledge(TODOHistory)  # a copy: changes need write_knowledge to stick
        _history.samples.append(reading["TODO_field"])
        self.write_knowledge(_history)
        #<!-- cc_code_monitor END--!>
        self.publish_event(event_key='new_data')    # LINK <outport> new_data

    def register_callbacks(self):
        self.register_event_callback(event_key='TODO_input_topic', callback=self.monitor)     # LINK <eventTrigger> TODO_input_topic


class Analysis(Node):

    def __init__(self, config, verbose=True, **kw):
        super().__init__(config=config, verbose=verbose, **kw)
        self._name = "Analysis"
        self.logger.info("Analysis instantiated")

    @timeit_callback
    def analyse(self, msg):
        #<!-- cc_code_analyse START--!>
        samples = self.read_knowledge(TODOHistory).samples
        anomaly = TODO_detect(samples)
        #<!-- cc_code_analyse END--!>
        if anomaly:
            # Write first, publish last: subscribers act on the event at once.
            _finding = TODOFinding()
            _finding.value = anomaly
            self.write_knowledge(_finding)
            self.publish_event(event_key='anomaly')    # LINK <outport> anomaly

    def register_callbacks(self):
        self.register_event_callback(event_key='new_data', callback=self.analyse)     # LINK <eventTrigger> new_data


class Plan(Node):

    def __init__(self, config, verbose=True, **kw):
        super().__init__(config=config, verbose=verbose, **kw)
        self._name = "Plan"
        self.logger.info("Plan instantiated")

    def plan(self, msg):
        # Keep this shape if planning is slow: the callback runs on the node's only listener thread.
        threading.Thread(target=self.plan_thread, args=(msg,), daemon=True).start()

    @timeit_callback
    def plan_thread(self, msg):
        #<!-- cc_code_plan START--!>
        finding = self.read_knowledge(TODOFinding).value
        _plan = TODOPlan()
        _plan.action = TODO_make_plan(finding)
        self.write_knowledge(_plan)
        #<!-- cc_code_plan END--!>
        self.publish_event(event_key='new_plan')    # LINK <outport> new_plan

    def register_callbacks(self):
        self.register_event_callback(event_key='anomaly', callback=self.plan)     # LINK <eventTrigger> anomaly


class Legitimate(Node):

    def __init__(self, config, verbose=True, **kw):
        super().__init__(config=config, verbose=verbose, **kw)
        self._name = "Legitimate"
        self.logger.info("Legitimate instantiated")

    @timeit_callback
    def legitimize(self, msg):
        #<!-- cc_code_legitimize START--!>
        action = self.read_knowledge(TODOPlan).action
        #<!-- cc_code_legitimize END--!>
        if TODO_is_safe(action):
            self.publish_event(event_key='isLegit')    # LINK <outport> isLegit
        else:
            self.publish_event(event_key='anomaly')    # LINK <outport> anomaly

    def register_callbacks(self):
        self.register_event_callback(event_key='new_plan', callback=self.legitimize)     # LINK <eventTrigger> new_plan


class Execute(Node):

    def __init__(self, config, verbose=True, **kw):
        super().__init__(config=config, verbose=verbose, **kw)
        self._name = "Execute"
        self.logger.info("Execute instantiated")

    @timeit_callback
    def execute(self, msg):
        #<!-- cc_code_execute START--!>
        action = self.read_knowledge(TODOPlan).action
        TODO_apply(action)
        #<!-- cc_code_execute END--!>

    def register_callbacks(self):
        self.register_event_callback(event_key='isLegit', callback=self.execute)     # LINK <eventTrigger> isLegit


def main():
    with open('TODO_config.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # register_callbacks() has to come before start(): the Redis listener only
    # starts if topics are already registered.
    nodes = []
    monitor_node = Monitor(config['Monitor_Config'])
    monitor_node.register_callbacks()
    monitor_node.start()
    nodes.append(monitor_node)

    analysis_node = Analysis(config['Analysis_Config'])
    analysis_node.register_callbacks()
    analysis_node.start()
    nodes.append(analysis_node)

    plan_node = Plan(config['Plan_Config'])
    plan_node.register_callbacks()
    plan_node.start()
    nodes.append(plan_node)

    legitimate_node = Legitimate(config['Legitimate_Config'])
    legitimate_node.register_callbacks()
    legitimate_node.start()
    nodes.append(legitimate_node)

    execute_node = Execute(config['Execute_Config'])
    execute_node.register_callbacks()
    execute_node.start()
    nodes.append(execute_node)
    return nodes


if __name__ == '__main__':
    import time
    nodes = main()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        for node in nodes:
            node.shutdown()
