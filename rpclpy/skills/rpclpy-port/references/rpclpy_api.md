# rpclpy API contract

What generated nodes must respect, and why. Rule ids match `check_rpclpy_api.py` output.
If this file and `rpclpy/*.py` disagree, the source wins; the checker reads signatures from it.
Sections 1 to 6 describe plain nodes. Section 7 lists what changes when a node has `Observation_Config`.

## Contents
1. Node lifecycle
2. Shared knowledge
3. Events and messages
4. Callbacks and threads
5. Coupling rules
6. Config
7. Observed nodes
8. Rule index

## 1. Node lifecycle

```python
node = Monitor(config['Monitor_Config'])   # a dict slice, never a filename
node.register_callbacks()                  # before start()
node.start()
```

- `Node.__init__(self, config, verbose=False, *, observation_backend=None)` does `self.config = config` with no file loading.
  A path string fails on `self.config['Logger_Config']` with `TypeError: string indices must be integers`.
- `register_callbacks()` must come before `start()`. `_start_redis` only starts the listener thread
  `if self.subscribe_topics`, so a node that registers afterwards is subscribed but never reads anything.
  There is no error: the node just stays silent.
- `self.knowledge`, `self.event_manager`, `self.message_manager` and `self.logger` exist only after
  `super().__init__`.

## 2. Shared knowledge

`read_knowledge` and `write_knowledge` dispatch on the type of their first argument:

| Call | Serialization | Key | Returns |
|---|---|---|---|
| `write_knowledge(instance)` | pickle | `instance.name` | backend result |
| `read_knowledge(Class)` | pickle | `Class().name` | a **copy** of the instance, or `None` if never written |
| `write_knowledge("k", value)` | none | `"k"` | backend result |
| `read_knowledge("k")` | `json.loads` if it parses, else `str` | `"k"` | value or `None` |

Use the class form by default, since it handles any picklable structure.

Things that go wrong:

- **Pass the class to read, an instance to write.** `read_knowledge(Cls())` tries to call the instance.
  `write_knowledge(Cls)` fails because `name` is an instance attribute.
- **Seed before reading.** Since 1.0.3 the class form returns `None` for a key that was never written (older
  versions raise `TypeError`). `read_knowledge(Cls).samples` therefore still raises, now as `AttributeError`, and ends
  the listener. Seed every class in a node `__init__`, and construct the seeding node first.
- **Reads are copies.** Mutating what you read changes nothing until `write_knowledge` sends it back.
- **The string form does not encode.** Despite what the reader suggests, `write_knowledge("k", {...})`
  passes the dict straight to the backend. Redis raises `DataError` on containers and `None`, while the
  `local` backend stores `str(value)`, which is not JSON. A smoke test can therefore pass on code that
  fails on Redis. `json.dumps` first, or use a class.
- **Do not mix forms on one key.** A string read of a pickled value tries to UTF-8 decode the pickle.
- **Read-modify-write is not atomic.** Two nodes appending to one list can lose an update. Give each
  list one writer, usually Monitor.
- **`self.knowledge.write(key, value)` is the raw layer.** It has no `cls=` parameter. The old
  `self.knowledge.write(cls=obj)` form raises `TypeError`.

Knowledge class requirements: no required `__init__` arguments, a unique string literal `name`, `uid` and
`timestamp` attributes, defined at module top level in a messages module. A class defined in a script is
pickled as `__main__.X`, which other processes cannot load. Attribute values must be picklable.

## 3. Events and messages

```python
self.publish_event('new_data')                      # str topic, optional dict
self.publish_event('anomaly', {'score': 3.2})
self.publish_message(status_instance)               # topic = obj.topic, else obj.name
self.publish_event(EventClass)                      # the class, which must declare .topic
```

Both publish methods also accept the keyword-only `evidence=`, which only has an effect on observed nodes (section 7).

- The payload on the wire is always a JSON string, stamped with `uid` and `timestamp`. Callbacks must
  `json.loads(msg)`, and every value must be JSON-serializable. Convert numpy with `.tolist()` and `float()`.
- An object is published as `json.dumps(obj.__dict__)`. Attributes behind a `_private` + property pair show
  up on the wire with the underscore, so use plain public attributes in published classes.
- Write knowledge **before** publishing. On the `local` bus delivery is synchronous, and on Redis it is a
  race: the subscriber can run before your next line. Publish last.
- The event and message planes are separate managers. A topic published on one reaches a subscriber on
  the other only when both planes use the same transport, as they do in the config `gen_config.py` writes.

### Topic chain

```
<input> -> Monitor -new_data-> Analysis -anomaly-> Plan -new_plan-> Legitimate -isLegit-> Execute
                                                     ^                  |
                                                     +---- anomaly -----+   (re-plan if rejected)
```

Keep these topic names unless the target system has its own. `rpclpy/templates/MAPLEK_template.py` uses longer
names (`observation_recorded`, `anomaly_detected`, `plan_generated` and so on), which are equally valid: the chain matters,
not the names. Every subscription needs a publisher, or must be declared with `--external-topic`.

## 4. Callbacks and threads

- A callback has the shape `def cb(self, msg)`, with `msg` the JSON string.
- **Any exception from a callback kills the node.** The `try` in `listen()` wraps the whole loop, so every exception
  ends the listener thread. `ValueError` and `OSError` do it quietly, with one printed line, and that includes
  `json.JSONDecodeError` from a malformed message. Anything else escapes the thread. Either way the process stays up
  while that node never fires again. Validate inputs, and handle failures you expect.
- Each manager has **one** listener thread. A slow callback queues every later event for that node. Hand
  long work to `threading.Thread(target=self.x_thread, args=(msg,), daemon=True)`, the way `Plan.plan` does in the node template.
- `@timeit_callback` writes `<Class>:start_execution` and `<Class>:execution_time`, which the dashboard Gantt
  reads. Put it on the callback, or on the thread target when the callback only spawns a thread.

## 5. Coupling rules

These are what make a port an integration into the architecture, not five classes in one file.

- Nodes communicate **only** through knowledge and events. No `global`, no shared module-level mutables, no
  node holding a reference to another node, and no direct calls between nodes.
- A node may keep private working state on `self` (a loaded model cache, say). Anything another stage or a
  later run needs goes in knowledge.
- Use the Node wrappers rather than opening a Redis client or calling `self.event_manager.publish`, so the
  transport stays swappable through config.

## 6. Config

Each node needs a section with `Logger_Config` (`log_level`, `format`, `logger_type`, plus a block for that
type), `Event_Manager_Config` (`protocol`, plus `host` and `port` for redis), `Message_Manager_Config`
(same), and `Knowledge_Config` (`knowledge_type`, plus `host`, `port` and `db` for redis).

All nodes must share one knowledge store and one event transport. The dashboard expects the Redis log channel
`<Component>:logs`. The `knowledgeIn` and `eventOut` blocks in `rpclpy/templates/Config_template.yaml` are not read at runtime.
`local` is valid for protocol and knowledge type, and the smoke test uses it.

## 7. Observed nodes

A node section containing `Observation_Config` records callbacks, typed knowledge access and publications in journals, through
the `rpclpy.rpio_observation` package. The checker's rules and the node template assume plain nodes. For an observed node, read the
[rpio-observation README](https://github.com/INTO-CPS-Association/rpio-observation#readme) too, and expect these differences:

- **Create the run first.** Call `create_run(namespace, ...)` before constructing nodes, and use a fresh namespace per run. Redis 7
  is required. Local nodes share one `MemoryJournal`, passed as `observation_backend=journal`, so node `__init__` must forward it.
- **Callbacks return an outcome.** Each returns an `outcomes` object such as `Monitored(obj)`, `Nominal()`, `Anomaly(obj)` or
  `Generated(obj)`, naming knowledge it read or wrote. Returning `None` fails the activation (O01).
- **Stay on the callback's thread.** After `start()`, typed knowledge access and publishing raise outside a callback's own thread,
  so the threaded `Plan` shape in the template does not work (O02).
- **Knowledge is JSON, not pickle.** Values must be JSON-serializable and live under `{namespace}:latest`, so anything reading the
  plain key, such as a `tc_bridge` or the dashboard, does not see them.
- **Evidence is exact.** `evidence={name: obj}` accepts only objects this activation read or wrote.
- **Check the flag.** `node.observation_failed` must be false before accepting a run.

The smoke test removes `Observation_Config`, so it exercises stage logic and not the journals. An observed port needs its own run
against `MemoryJournal`.

## 8. Rule index

| Rule | Sev. | Meaning |
|---|---|---|
| N00 | E | no `Node` subclass found |
| N01 | E | `__init__` does not call `super().__init__` |
| N02 | W | `config` defaults to a string |
| N03 | W | no `register_callbacks` |
| N04 | E | `self.x` exists on neither the class nor `Node` |
| N05 | E | call does not match the real rpclpy signature |
| N06 | E | method does not exist on the manager |
| N07 | W | bypasses a Node wrapper |
| N08 / N09 | E | `register_callbacks()` / `start()` never called |
| N10 | E | `start()` before `register_callbacks()` |
| N11 | E | node constructed without a config dict |
| K01 | E | knowledge class `__init__` has required arguments |
| K02 / K03 | E | `name` missing or not a literal / duplicate key |
| K04 / K05 | W | key differs from class name / no `uid` or `timestamp` |
| K06 / K07 | E / W | nested class / class defined in the node script |
| K08 | W | knowledge mutated but not written back |
| K09 | W | attribute not declared on the class |
| K10 / K11 | E | `read_knowledge` given an instance / unknown class |
| K12 | E | string-key write of `None` or a container |
| K13 | E | `write_knowledge` given a class |
| K14 / K15 | E / W | read but never written / not seeded in an `__init__` |
| K16 | E | one key used in both string and pickle form |
| E01 | E | message is not a dict |
| E02 / E03 | E | `publish_event` given an instance / class without `.topic` |
| E04 | W | underscore attributes leak onto the wire |
| C01 / C02 | E | callback missing / wrong signature |
| C03 / C04 | W | no `@timeit_callback` / blocks the listener thread |
| L01 | W | knowledge written after publishing (race) |
| A01 / A03 | E | `global` state / node creates a node |
| A02 / A04 | W | state kept on `self` / own Redis client |
| T01 / T02 | E / W | subscribed with no publisher / published with no subscriber |
| T03 / T04 | E | MAPLE-K stage missing / chain broken between stages |
| Y01 – Y03 | E | config section or key missing / unsupported value |
| Y04 | W | log channel is not `<Component>:logs` |
| Y05 – Y07 | E | nodes do not share a store or transport |
| M01 / M02 | W | `# LINK` comment disagrees with its topic / unbalanced cc markers |
| O01 / O02 | E | observed callback returns no outcome / observed work on a spawned thread |

Treat L01 and K08 as errors in generated code. They are warnings only because existing code may have reasons
the checker cannot see, and code you just wrote does not.
