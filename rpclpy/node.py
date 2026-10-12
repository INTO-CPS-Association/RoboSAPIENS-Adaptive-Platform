import yaml
import logging
import json
import uuid
import pickle
from datetime import datetime
from rpclpy.CommunicationManager import CommunicationManager
from rpclpy.KnowledgeManager import KnowledgeManager
from rpclpy.LoggingAndTracking import LoggingAndTrackingHandler


# ---------------------------------------------------------------------------
# Configuration Keys
# ---------------------------------------------------------------------------
KEY_EVENT_MANAGER_CONFIG = "Event_Manager_Config"
KEY_MESSAGE_MANAGER_CONFIG = "Message_Manager_Config"
KEY_LOGGER_CONFIG = "Logger_Config"
KEY_OBSERVATION_CONFIG = "Observation_Config"
KEY_KNOWLEDGE_CONFIG = "Knowledge_Config"
KEY_LOG_LEVEL = "log_level"
KEY_LOG_FORMAT = "format"
KEY_PROTOCOL = "protocol"

# ---------------------------------------------------------------------------
# Default Values and Messages
# ---------------------------------------------------------------------------
ENCODING_UTF8 = "utf-8"

WARN_EVENT_MANAGER_NOT_SET = "Event manager is not set for Event publishing."
WARN_REGISTER_EVENT_CALLBACK_NOT_SET = "Event manager is not set for registering event callbacks."

MSG_INITIALIZING_KNOWLEDGE = "Initializing Knowledge: {} knowledge"
MSG_INITIALIZING_EVENT_MANAGER = "Initializing Event Manager"
MSG_INITIALIZING_IO_MANAGER = "Initializing IO Manager"
MSG_USING_COMMUNICATION_MANAGER = "{} is using {} Communication Manager"
MSG_STARTING = "{} is starting..."
MSG_SHUTTING_DOWN = "{} is shutting down..."
MSG_REGISTER_EVENT_CALLBACK = "Registered callback for event: {}"


class Node:
    def __init__(self, config, verbose=False, *, observation_backend=None):
        # self.config = self.load_config(config)
        self.config = config
        self.logger = self._initialize_logger()
        self._observation = self._initialize_observation(observation_backend)
        self.knowledge = self._initialize_knowledge()  # Initialize knowledge within the component
        self.event_manager = self._initialize_event_manager()
        self.message_manager = self._initialize_message_manager()

        # Initialize MQTT and ROS2 Event
        if self.event_manager:
            self.logger.info(
                MSG_USING_COMMUNICATION_MANAGER.format(
                    self.__class__.__name__,
                    self.config[KEY_EVENT_MANAGER_CONFIG][KEY_PROTOCOL]
                )
            )
        if self.message_manager:
            self.logger.info(
                MSG_USING_COMMUNICATION_MANAGER.format(
                    self.__class__.__name__,
                    self.config[KEY_MESSAGE_MANAGER_CONFIG][KEY_PROTOCOL]
                )
            )

    def load_config(self, config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def _initialize_logger(self):
        # Create the main logger.
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(self.config[KEY_LOGGER_CONFIG][KEY_LOG_LEVEL])
        # Instantiate the custom logging handler.
        custom_handler = LoggingAndTrackingHandler(self.config[KEY_LOGGER_CONFIG])
        formatter = logging.Formatter(self.config[KEY_LOGGER_CONFIG][KEY_LOG_FORMAT])
        custom_handler.setFormatter(formatter)
        logger.addHandler(custom_handler)
        return logger

    def _initialize_knowledge(self):
        """Initialize the Knowledge object based on the config."""
        self.logger.info(
            MSG_INITIALIZING_KNOWLEDGE.format(
                self.config[KEY_KNOWLEDGE_CONFIG]["knowledge_type"]
            )
        )
        return KnowledgeManager(self.config[KEY_KNOWLEDGE_CONFIG])

    def _initialize_observation(self, backend):
        """Attach rpio_observation when ``Observation_Config`` is present."""
        config = self.config.get(KEY_OBSERVATION_CONFIG)
        if config is None:
            return None
        from rpclpy.rpio_observation.errors import ObservationError
        from rpclpy.rpio_observation.journal import MemoryJournal, RedisJournal
        from rpclpy.rpio_observation.observation import NodeObservation
        knowledge = self.config[KEY_KNOWLEDGE_CONFIG]
        if knowledge["knowledge_type"] == "redis" and backend is None:
            backend = RedisJournal(knowledge, config["namespace"])
        elif knowledge["knowledge_type"] != "local" or not isinstance(backend, MemoryJournal):
            raise ObservationError("observation needs redis Knowledge or local Knowledge "
                                   "with observation_backend=MemoryJournal()")
        return NodeObservation(self.__class__.__name__, config, backend)

    @property
    def observation_failed(self):
        """True once any observation error or callback failure occurred."""
        return self._observation is not None and self._observation.failed

    def _initialize_event_manager(self):
        """Initialize the Event Manager based on the config."""
        self.logger.info(MSG_INITIALIZING_EVENT_MANAGER)
        return CommunicationManager(config=self.config[KEY_EVENT_MANAGER_CONFIG])

    def _initialize_message_manager(self):
        """Initialize the IO Manager based on the config."""
        self.logger.info(MSG_INITIALIZING_IO_MANAGER)
        return CommunicationManager(config=self.config[KEY_MESSAGE_MANAGER_CONFIG])

    def start(self):
        """Start the component and enable Event."""
        self.logger.info(MSG_STARTING.format(self.__class__.__name__))
        if self._observation is not None:
            self._observation.start()
        if self.event_manager:
            self.event_manager.start()
        if self.message_manager:
            self.message_manager.start()

    def shutdown(self):
        """Shutdown the component and stop Event."""
        self.logger.info(MSG_SHUTTING_DOWN.format(self.__class__.__name__))
        if self.event_manager:
            self.event_manager.stop()
        if self.message_manager:
            self.message_manager.stop()
        if self._observation is not None:
            self._observation.backend.close()

    def _publish_observed(self, kind, manager, topic, message, evidence):
        stamps = self._observation.publish(kind, topic, message, evidence)
        wire = dict(message or {}, uid=str(uuid.uuid4().hex[:8]),
                    timestamp=datetime.now().isoformat(), **stamps)
        if manager:
            manager.publish(topic, json.dumps(wire))
        else:
            self.logger.warning(WARN_EVENT_MANAGER_NOT_SET)

    def publish_event(self, event_key, message=None, *, evidence=None):
        """Publish Event using the Event manager.

        Observed nodes: ``evidence`` maps names to Knowledge objects this
        activation read or wrote; the Event cites exact versions and is journalled before it is sent.
        """
        if self._observation is not None:
            if not isinstance(event_key, str):
                instance = event_key()
                event_key, message = instance.topic, instance.__dict__
            return self._publish_observed("event", self.event_manager, event_key, message,
                                          evidence)
        if isinstance(event_key, str):
            # Copy so the caller's dict is not mutated and no state is shared
            # between calls.
            message = dict(message) if message else {}
            # Generate a unique ID for the message
            message_id = str(uuid.uuid4().hex[:8])
            # Get the current timestamp in ISO 8601 format
            timestamp = datetime.now().isoformat()

            message['uid'] = message_id
            message['timestamp'] = timestamp

            if self.event_manager:
                self.event_manager.publish(event_key,  json.dumps(message))
            else:
                self.logger.warning(WARN_EVENT_MANAGER_NOT_SET)
        else: 
            event_cls = event_key()
            event_cls.uid = str(uuid.uuid4().hex[:8])
            event_cls.timestamp = datetime.now().isoformat()
            value = json.dumps(event_cls.__dict__)
            self.event_manager.publish(event_cls.topic, value)



    def publish_message(self, message_key, message=None, *, evidence=None):
        """Publish Message using the Message manager (see publish_event)."""
        if self._observation is not None:
            if not isinstance(message_key, str):
                instance = message_key if (hasattr(message_key, '__dict__')
                                           and hasattr(message_key, 'name')) else message_key()
                message_key, message = getattr(instance, 'topic', instance.name), instance.__dict__
            return self._publish_observed("message", self.message_manager, message_key,
                                          message, evidence)
        if isinstance(message_key, str):
            # Copy so the caller's dict is not mutated and no state is shared
            # between calls.
            message = dict(message) if message else {}
            # Generate a unique ID for the message
            message_id = str(uuid.uuid4().hex[:8])
            # Get the current timestamp in ISO 8601 format
            timestamp = datetime.now().isoformat()

            message['uid'] = message_id
            message['timestamp'] = timestamp

            if self.message_manager:
                self.message_manager.publish(message_key, json.dumps(message))
            else:
                self.logger.warning(WARN_EVENT_MANAGER_NOT_SET)
        else:
            # Check if message_key is already an instance (has __dict__ attribute)
            if hasattr(message_key, '__dict__') and hasattr(message_key, 'name'):
                # It's already an instance, use it directly
                message_cls = message_key
            else:
                # It's a class, instantiate it
                message_cls = message_key()
            
            # Set uid and timestamp
            message_cls.uid = str(uuid.uuid4().hex[:8])
            message_cls.timestamp = datetime.now().isoformat()
            
            # Serialize and publish
            value = json.dumps(message_cls.__dict__)
            
            if self.message_manager:
                # Use the topic attribute if available, otherwise use name
                topic = getattr(message_cls, 'topic', message_cls.name)
                self.message_manager.publish(topic, value)
            else:
                self.logger.warning(WARN_EVENT_MANAGER_NOT_SET)

    def register_event_callback(self, event_key, callback):
        """Register a callback for Event manager events (MQTT or Redis)."""
        if self.event_manager:
            if self._observation is not None:
                callback = self._observation.wrap("event", event_key, callback)
            self.event_manager.subscribe(event_key, callback)
            self.logger.info(MSG_REGISTER_EVENT_CALLBACK.format(event_key))
        else:
            self.logger.warning(WARN_REGISTER_EVENT_CALLBACK_NOT_SET)

    def register_message_callback(self, event_key, callback):
        """Register a callback for Event manager events (MQTT or Redis)."""
        if self.message_manager:
            if self._observation is not None:
                callback = self._observation.wrap("message", event_key, callback)
            self.message_manager.subscribe(event_key, callback)
            self.logger.info(MSG_REGISTER_EVENT_CALLBACK.format(event_key))
        else:
            self.logger.warning(WARN_REGISTER_EVENT_CALLBACK_NOT_SET)

    def read_knowledge(self, key, queueSize=1):
        """Read a value from the Knowledge Manager."""
        if isinstance(key, str):
            value = self.knowledge.read(key, queueSize)
            if value is not None:
                value = value.decode(ENCODING_UTF8)  # Convert bytes to string
                try:
                    value = json.loads(value)  # Try to deserialize the value if it's a JSON string
                except json.JSONDecodeError:
                    pass  # If it's not JSON, return it as a string
            
            return value
        else:
            return self.read_knowledge_as_object(key)
    
    def read_knowledge_as_object(self, cls):
        if self._observation is not None:
            return self._observation.read(cls)
        _cls = cls()
        value = self.knowledge.read(_cls.name)
        if value is None:
            return None   # never written: ordinary absence, as for raw reads
        return pickle.loads(value)

    def write_knowledge(self, key, value = None):
        """Write a value to the Knowledge Manager."""
        if isinstance(key, str):
            if isinstance(value, str):
                value = str(value)
            return self.knowledge.write(key, value)
        else:
            return self.write_knowledge_as_object(key)
        
    def write_knowledge_as_object(self, cls):
        if self._observation is not None:
            return self._observation.write(cls)
        # """Write a class instance to the Knowledge Manager."""
        # value = json.dumps(cls.__dict__)
        cls.uid = str(uuid.uuid4().hex[:8])
        cls.timestamp = datetime.now().isoformat()
        value = pickle.dumps(cls)
        return self.knowledge.write(cls.name, value)

    def service_started(self, service, request_id=None):
        """Record that the current observed activation starts providing ``service``."""
        if self._observation is not None:
            return self._observation.service(service, request_id, None)

    def service_completed(self, service, request_id=None, outcome="Delivered"):
        """Record completion (``Delivered`` or ``NotDelivered``) of ``service``."""
        if self._observation is not None:
            return self._observation.service(service, request_id, outcome)
