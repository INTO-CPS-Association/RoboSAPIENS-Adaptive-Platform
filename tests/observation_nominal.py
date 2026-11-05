"""A small observed Monitor -> Analysis run, shared by the tests and the fixture.

    python tests/observation_nominal.py tests/fixtures/journal-v2/nominal.jsonl
"""
import json
import sys

from rpclpy.CommunicationManager import LocalBus
from rpclpy.KnowledgeManager import reset_local_stores
from rpclpy.node import Node
from rpclpy.rpio_observation import MemoryJournal, create_run, dump, outcomes

EPOCH = "nominal-epoch"


class Settings:                      # written once during setup
    name = "Settings"

    def __init__(self):
        self.threshold = 5.0


class Reading:
    name = "Reading"

    def __init__(self, value=0.0):
        self.value = value
        self.tcp_pose = None


class Monitor(Node):
    def __init__(self, config, **kw):
        super().__init__(config, **kw)
        self.write_knowledge(Settings())
        self.register_message_callback("/sensor", self.on_sensor)

    def on_sensor(self, message):
        reading = Reading(json.loads(message)["value"])
        self.write_knowledge(reading)
        self.publish_event("reading_recorded", {"value": reading.value},
                           evidence={"reading": reading})
        return outcomes.Monitored(reading)


class Analysis(Node):
    def __init__(self, config, **kw):
        super().__init__(config, **kw)
        self.register_event_callback("reading_recorded", self.on_reading)

    def on_reading(self, message):
        self.read_knowledge(Reading)
        return outcomes.Nominal()


def config(namespace, knowledge, **observation):
    return {
        "Logger_Config": {"log_level": "WARNING", "logger_type": "terminal",
                          "format": "%(message)s"},
        "Event_Manager_Config": {"protocol": "local", "db": 0},
        "Message_Manager_Config": {"protocol": "local", "db": 1},
        "Knowledge_Config": knowledge,
        "Observation_Config": dict(namespace=namespace, loop=["demo"], **observation),
    }


def start_nominal(namespace, *, redis_config=None, node_classes=(Monitor, Analysis)):
    """Create the run and start the nodes; return ``(nodes, source)`` (``source`` feeds ``dump``)."""
    LocalBus.reset()
    reset_local_stores()
    if redis_config is None:
        source = MemoryJournal()
        create_run(namespace, backend=source, epoch=EPOCH)
        knowledge, kw = {"knowledge_type": "local", "db": 2}, {"observation_backend": source}
    else:
        source = redis_config
        create_run(namespace, redis_config=redis_config, epoch=EPOCH)
        knowledge, kw = dict(redis_config, knowledge_type="redis"), {}
    nodes = [cls(config(namespace, knowledge), **kw) for cls in node_classes]
    for node in nodes:
        node.start()
    return nodes, source


def run_nominal(namespace, *, values=(3.0,), **kwargs):
    """Start, publish one sensor message per value, shut down."""
    nodes, source = start_nominal(namespace, **kwargs)
    for value in values:
        LocalBus.get().publish("/sensor", json.dumps({"value": value}))
    for node in nodes:
        node.shutdown()
    return nodes, source


if __name__ == "__main__":
    nodes, source = run_nominal("nominal")
    assert not any(n.observation_failed for n in nodes)
    print(dump(source, "nominal", sys.argv[1]))
