# RPCLRS - RoboSapiens Client Library for Rust

A Rust library for creating nodes that can communicate with each other using messages, events, and knowledge management via Redis.

## Features

- **Async/Await Support**: Built with Tokio for high-performance async operations
- **Type Safety**: Strong type system with serde serialization
- **Communication**: Redis pub/sub for events and messages
- **Knowledge Management**: Redis key-value storage with object serialization
- **Thread Safety**: Arc/Mutex for concurrent operations
- **Error Handling**: Comprehensive error handling with anyhow and thiserror

## Dependencies

- Rust 1.70+
- Redis server

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
rpclrs = "1.0.0"
```

## Examples

### Sensor Monitor Example

This example demonstrates:

- Node-to-node communication using events and messages
- Knowledge management for storing and retrieving data
- Real-time sensor data publishing and monitoring

#### Run Examples

1. Start Redis: `redis-server`
2. Run monitor: `cargo run --example monitor_node`
3. Run sensor: `cargo run --example sensor_node` (in another terminal)

## API Overview

```rust
use rpclrs::Node;
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut node = Node::new("MyNode", "localhost", 6379)?;
    node.start()?;

    // Publish event
    let data = json!({"temperature": 22.5});
    node.publish_event("sensor/data", data)?;

    // Subscribe to events
    node.register_event_callback("alerts/high", |topic, message| {
        println!("Alert received: {}", message);
    })?;

    // Knowledge management
    node.write_knowledge("config", "value")?;
    if let Some(value) = node.read_knowledge("config")? {
        println!("Config: {}", value);
    }

    Ok(())
}
```

## Building

```bash
cargo build --release
```

## Running Tests

```bash
cargo test
```

## License

This software is owned by [The INTO-CPS Association](https://into-cps.org/) and is available under [the INTO-CPS License](../LICENSE.md).
