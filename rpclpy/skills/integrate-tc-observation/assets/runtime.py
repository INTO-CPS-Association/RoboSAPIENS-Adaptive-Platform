"""Failure policy for MAPLE-K nodes (template).

Observed node: re-raise so rpclpy journals an ``Aborted`` End and sets ``observation_failed``.
Unobserved node: log and keep listening (an uncaught exception kills rpclpy's listener thread).
"""
import functools
import threading

from rpclpy.node import Node

LOCK = threading.RLock()   # share one lock when several nodes run in one process


class MapleNode(Node):
    callback_failed = False

    def __init__(self, config=None, verbose=True, **options):   # options carries observation_backend
        super().__init__(config=config, verbose=verbose, **options)

    def safe_callback(self, callback):
        @functools.wraps(callback)
        def run(message):
            with LOCK:
                try:
                    return callback(message)
                except Exception:
                    self.callback_failed = True
                    if self._observation is not None:
                        raise
                    self.logger.exception("callback %s failed", callback.__name__)
        return run
