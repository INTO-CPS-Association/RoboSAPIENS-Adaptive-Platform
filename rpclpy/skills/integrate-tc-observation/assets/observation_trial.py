"""Trial harness (template). The order is the protocol:

fresh namespace -> create_run (BEFORE any node exists) -> build/start nodes -> workload ->
shutdown -> fail if any node failed -> dump journal + manifest. A failed trial writes no journal.
"""
import copy
import json
import pathlib
import uuid

from rpclpy.rpio_observation import MemoryJournal, create_run, dump


class TrialFailed(RuntimeError):
    pass


def trial_config(template, namespace, codec, knowledge):
    """Copy the template config and fill the per-trial Observation_Config keys."""
    config = copy.deepcopy(template)
    config["Knowledge_Config"] = knowledge
    config["Observation_Config"] = dict(config.get("Observation_Config", {}),
                                        namespace=namespace, codec=codec)
    return config


class ObservedTrial:
    """Memory transport. For Redis pass ``redis_config`` and drop ``observation_backend``."""

    def __init__(self, namespace, source, epoch, redis_config=None):
        self.namespace, self.source, self.epoch, self.redis_config = namespace, source, epoch, redis_config
        self.nodes = []

    @classmethod
    def open(cls, redis_config=None):
        namespace = f"trial-{uuid.uuid4().hex[:12]}"       # never reuse: epoch is bound to namespace
        if redis_config is None:
            source = MemoryJournal()
            epoch = create_run(namespace, backend=source)
        else:
            source = redis_config
            epoch = create_run(namespace, redis_config=redis_config)
        return cls(namespace, source, epoch, redis_config)

    def node_options(self):
        return {"observation_backend": self.source} if self.redis_config is None else {}

    def knowledge(self):
        if self.redis_config is None:
            return {"knowledge_type": "local", "db": 2}
        return dict(self.redis_config, knowledge_type="redis")

    def build(self, factory):
        """factory(trial) -> list of started nodes; construct everything, then start."""
        self.nodes = list(factory(self))
        return self.nodes

    def finish(self, out_dir, name):
        for node in self.nodes:
            node.shutdown()
        failed = [type(n).__name__ for n in self.nodes
                  if n.observation_failed or getattr(n, "callback_failed", False)]
        if failed:
            raise TrialFailed(f"callbacks failed in {failed}; no journal written")
        out = pathlib.Path(out_dir)
        out.mkdir(parents=True, exist_ok=True)
        manifest = dump(self.source, self.namespace, out / f"{name}.jsonl")
        manifest.update(namespace=self.namespace)
        (out / f"{name}.trial.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
        return manifest
