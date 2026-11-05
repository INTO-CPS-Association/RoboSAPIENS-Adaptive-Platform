#!/usr/bin/env python
"""Write a minimal rpclpy config: one section per node, all on one Redis.

    python gen_config.py my_config.yaml --nodes Monitor,Analysis,Plan,Legitimate,Execute

Only the keys rpclpy reads at runtime are emitted. The knowledgeIn/eventOut
blocks in rpclpy/templates/Config_template.yaml are not read by anything.
"""
import argparse
import os
import sys

import yaml

LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def section(component, host, port, db):
    return {
        "Logger_Config": {
            "log_level": "DEBUG",
            "logger_type": "redis",
            "format": LOG_FORMAT,
            "redis": {"host": host, "port": port, "channel": f"{component}:logs"},
        },
        "Event_Manager_Config": {"protocol": "redis", "host": host, "port": port},
        "Message_Manager_Config": {"protocol": "redis", "host": host, "port": port},
        "Knowledge_Config": {"knowledge_type": "redis", "host": host, "port": port, "db": db},
    }


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("output")
    p.add_argument("--nodes", default="Monitor,Analysis,Plan,Legitimate,Execute")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=6379)
    p.add_argument("--db", type=int, default=0)
    p.add_argument("--force", action="store_true", help="overwrite an existing file")
    args = p.parse_args(argv)
    if os.path.exists(args.output) and not args.force:
        print(f"{args.output} exists; pass --force to overwrite", file=sys.stderr)
        return 1
    config = {f"{n.strip()}_Config": section(n.strip(), args.host, args.port, args.db) for n in args.nodes.split(",")}
    with open(args.output, "w", encoding="utf-8") as f:
        yaml.safe_dump(config, f, sort_keys=False)
    print(f"wrote {args.output}: {', '.join(config)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
