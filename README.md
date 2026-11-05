# RoboSAPIENS Adaptive Platform (RAP)

The **RoboSAPIENS Adaptive Platform (RAP)** is a middleware framework for building self-adaptive robotic systems. It provides a structured runtime for implementing the **MAPLE-K** (Monitor, Analysis, Plan, Legitimate, Execute – Knowledge) autonomic control loop, enabling robotic applications to continuously observe their environment, reason about their state, and adapt their behavior at runtime — without human intervention.

RAP is developed as part of the [RoboSAPIENS](https://robosapiens-eu.tech/) project, a European research initiative focused on trustworthy and self-adaptive human-robot systems. It is designed to be use-case agnostic: the same platform powers different RoboSAPIENS applications, from swarm robotics to human-robot collaboration scenarios.

---

## rpclpy — Python Client Library for RAP

**rpclpy** is the official Python client library for RAP. It gives developers the building blocks to implement MAPLE-K loop nodes as distributed Python processes that communicate via events and messages, and share context through a common Knowledge base.

[![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)](https://python.org)
[![Redis](https://img.shields.io/badge/Redis-6.0+-red.svg)](https://redis.io)
[![PyPI](https://img.shields.io/pypi/v/rpclpy.svg)](https://pypi.org/project/rpclpy/)

## MAPLE-K Loop in RoboSAPIENS

```sh

```
```raw
┌──────────────────────────────────────────────────────────────────┐
│                     MAPLE-K Loop (RAP)                           │
│                                                                  │
│   ┌─────────┐   ┌─────────┐   ┌──────┐   ┌───────────┐           │
│   │ Monitor │──►│Analysis │──►│ Plan │──►│ Legitimate│──┐        │
│   └─────────┘   └─────────┘   └──────┘   └───────────┘  │        │
│                                                         ▼        │
│                                                    ┌─────────┐   │
│                                                    │ Execute │   │
│                                                    └─────────┘   │
│                                                                  │
│  ─────────────────────── Knowledge ──────────────────────────    │
└──────────────────────────────────────────────────────────────────┘
```

Each MAPLE-K element runs as an independent **Node** built with rpclpy. Nodes share state via the **Knowledge** layer, and coordinate transitions via **Events** and **Messages** — all managed through a distributed, multi-protocol communication backend.

## Features

- **Node Communication**: Publish/subscribe messaging and events with automatic metadata
- **Knowledge Management**: Distributed key-value storage with object serialization
- **Redis Integration**: Fast, reliable backend for all operations across multiple databases
- **Real-time Communication**: Event-driven architecture with thread-safe operations
- **Flexible Protocols**: Redis, MQTT, RabbitMQ, Kafka, WebSocket, TCP, Zenoh
- **Multiple Knowledge Backends**: Redis, Memcached, Ignite, Hazelcast, Tarantool, Aerospike, SQLite
- **Logging & Monitoring**: Built-in structured logging with configurable levels (terminal, file, MQTT, Redis)
- **UUID & Timestamps**: Automatic message identification and timestamping
- **Dashboard**: Built-in web dashboard via Dash for real-time monitoring

## Installation

```bash
pip install rpclpy
```

Install optional extras based on what you need:

```bash
pip install rpclpy[mqtt]       # MQTT protocol / logging
pip install rpclpy[kafka]      # Kafka protocol / backend
pip install rpclpy[rabbitmq]   # RabbitMQ protocol
pip install rpclpy[websocket]  # WebSocket protocol
pip install rpclpy[dashboard]  # Web dashboard (Dash + Plotly)
pip install rpclpy[all]        # Everything
```

To work from source, clone with the `rpio-observation` submodule:

```bash
git clone --recurse-submodules https://github.com/INTO-CPS-Association/RoboSAPIENS-Adaptive-Platform.git
# In an existing checkout:
git submodule update --init
```

Only nodes with `Observation_Config` and the observation tests need the submodule.

## Repository Structure

```raw
rpclpy/
├── __init__.py               # Library entry point
├── node.py                   # Main Node class
├── CommunicationManager.py   # Multi-protocol pub/sub
├── KnowledgeManager.py       # Knowledge storage backends
├── LoggingAndTracking.py     # Logging infrastructure
├── DashboardApp.py           # Web monitoring dashboard
├── utils.py                  # Utility decorators
└── rpio_observation/         # Optional reference journals (git submodule)
```

## Prerequisites

- Python 3.10+
- Redis 6.0+ running on localhost:6379

**Install Redis:**

```bash
# Ubuntu/Debian
sudo apt update && sudo apt install redis-server
sudo systemctl start redis-server

# macOS
brew install redis && brew services start redis

# Docker
docker run --name redis-server -p 6379:6379 -d redis
```

## Configuration

rpclpy is fully configuration-driven via a YAML file. Redis databases are separated by concern:

- **DB 0**: Events (pub/sub)
- **DB 1**: Messages (pub/sub)
- **DB 2**: Knowledge (key-value)

```yaml
Logger_Config:
  log_level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  logger_type: "terminal"   # terminal | file | mqtt | redis

Event_Manager_Config:
  protocol: "redis"
  host: "localhost"
  port: 6379
  db: 0

Message_Manager_Config:
  protocol: "redis"
  host: "localhost"
  port: 6379
  db: 1

Knowledge_Config:
  knowledge_type: "redis"
  host: "localhost"
  port: 6379
  db: 2
```

### Local (in-process) mode — no broker required

For simulation, testing, or single-process deployments you can run an entire
MAPLE-K loop without Redis (or any other broker) by selecting the `local`
backend. All nodes in the process then share a global in-memory event bus and
knowledge store. Event delivery is **synchronous**: publishing an event invokes
the subscribed callbacks inline, so a single trigger drives the whole loop to
completion before returning. Application code is unchanged — only the config
differs:

```yaml
Logger_Config:
  log_level: "INFO"
  format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
  logger_type: "terminal"

Event_Manager_Config:
  protocol: "local"

Message_Manager_Config:
  protocol: "local"

Knowledge_Config:
  knowledge_type: "local"
```

## API Reference

| Method | Description |
|--------|-------------|
| `Node(config)` | Create a node from a config dict |
| `node.start()` | Start event and message managers |
| `node.shutdown()` | Stop all managers |
| `node.publish_event(topic, data)` | Publish an event |
| `node.register_event_callback(topic, fn)` | Subscribe to an event topic |
| `node.publish_message(topic, data)` | Publish a direct message |
| `node.register_message_callback(topic, fn)` | Subscribe to a message topic |
| `node.write_knowledge(key, value)` | Store a value |
| `node.read_knowledge(key)` | Retrieve a value |

### Optional reference journals

A node whose configuration contains `Observation_Config` records callbacks,
typed Knowledge access, publications and service milestones for runtime
verification. Phase facts and immutable Knowledge commits use separate journals;
a callback Write and its Knowledge commit are admitted atomically. Neither
journal is trimmed during a run. Business code keeps the APIs above.
Publications accept `evidence=`, and `node.service_started` /
`node.service_completed` record service milestones. The `node.observation_failed`
property reports an observation or callback failure. See the
[`rpio-observation` README](https://github.com/INTO-CPS-Association/rpio-observation#readme)
for configuration, run/capture lifecycle and failure semantics. Without `Observation_Config`
behaviour is unchanged, except that reading a typed Knowledge object that was
never written returns `None` instead of failing.

## Architecture

Each node in the MAPLE-K loop is a separate rpclpy `Node` instance. They communicate in real time and share knowledge through a distributed backend.

```raw
  MAPLE-K Nodes (each is an rpclpy Node)

┌──────────┐  ┌──────────┐  ┌──────┐  ┌───────────┐  ┌─────────┐
│ Monitor  │  │ Analysis │  │ Plan │  │ Legitimate│  │ Execute │
└────┬─────┘  └────┬─────┘  └──┬───┘  └─────┬─────┘  └────┬────┘
     │             │           │            │             │
     └─────────────┴───────────┴────────────┴─────────────┘
                              Events / Messages
                                    │
                                    ▼
          ┌─────────────────────────────────────────────┐
          │               RAP Backend                   │
          │  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │
          │  │ Events  │  │Messages │  │  Knowledge  │  │
          │  │ (DB 0)  │  │ (DB 1)  │  │   (DB 2)    │  │
          │  │ Pub/Sub │  │ Pub/Sub │  │  Key-Value  │  │
          │  └─────────┘  └─────────┘  └─────────────┘  │
          └─────────────────────────────────────────────┘
```

## Use Cases — RAP Integration

### 1. PAL Robotics

- [pal_maple-k_demo](https://gitlab.com/pal-robotics/robosapiens/pal_maple-k_demo)
- [pal_fleet](https://gitlab.com/pal-robotics/robosapiens/pal_fleet)

### 2. NTNU Ship Navigation

- [rpio-ShipNavigation](https://github.com/saharnn96/rpio-ShipNavigation)

### 3. DTI Screw Detection

- [screw_detection](https://github.com/TI-RoboSapiens/screw_detection)

### 4. TurtleBot

---

## Troubleshooting

**Redis connection failed:**
```bash
redis-cli ping   # should return PONG
sudo systemctl start redis-server   # Linux
brew services start redis           # macOS
```

**Import errors after install:**
```bash
# Verify the install
python -c "import rpclpy; print(rpclpy.__version__)"
```

## Version

**Current version: 1.0.0**

### Changelog

- **v1.0.0** (2025-07-31): Initial release
  - Core node communication (events, messages, knowledge)
  - Multi-protocol support: Redis, MQTT, RabbitMQ, Kafka, WebSocket, TCP, Zenoh
  - Multiple knowledge backends
  - Thread-safe operations
  - Structured logging and web dashboard

---

**Built with ❤️ by the RoboSAPIENS Team**

For questions, support, or contributions, visit the [GitHub repository](https://github.com/saharnn96/robosapiens-clientlibraries).

---

## License

This software is owned by [The INTO-CPS Association](https://into-cps.org/) and is available under [the INTO-CPS License](./LICENSE.md).



