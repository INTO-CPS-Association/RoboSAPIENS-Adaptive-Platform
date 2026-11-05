import yaml
from rpclpy.DashboardApp import run_dashboard
from rpclpy.node import Node
from rpclpy.utils import timeit_callback
import time
import threading
from messages import *

# TODO: Add your use-case specific imports here
# e.g.:
# import numpy as np
# import json


class Monitor(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Monitor"
        self.logger.info("Monitor instantiated")

        # Initialize shared knowledge
        _baseline_score = BaselineScore()
        _baseline_score.score = None
        self.write_knowledge(_baseline_score)

        _ratio_list = AnomalyRatioList()
        _ratio_list.ratio_list = []
        self.write_knowledge(_ratio_list)

        _obs_list = ObservationList()
        _obs_list.observations = []
        self.write_knowledge(_obs_list)

        _in_planning = InPlanning()
        _in_planning.in_planning = False
        self.write_knowledge(_in_planning)

        _abnormal_start_idx = AbnormalStartIdx()
        _abnormal_start_idx.abnormal_start_idx = None
        self.write_knowledge(_abnormal_start_idx)

        _replanning_counter = ReplanningCounter()
        _replanning_counter.replanning_counter = 0
        self.write_knowledge(_replanning_counter)

    @timeit_callback
    def monitor(self, msg):
        """
        Called each time a new sensor reading arrives.

        Steps to implement:
          1. Parse `msg` into your sensor data structure.
          2. Compute a performance score (e.g., prediction error, deviation).
          3. Read BaselineScore; set it on the first call.
          4. Compute ratio = current_score / baseline_score and append to AnomalyRatioList.
          5. Append the raw observation to ObservationList.
          6. If not in planning, publish 'new_data' to trigger Analysis.
        """
        # TODO: implement monitoring logic, e.g.:
        #
        # import json
        # data = json.loads(msg)
        # observation = data['<your_field>']
        # score = <compute_score>(observation)
        #
        # baseline_obj = self.read_knowledge(BaselineScore)
        # if baseline_obj.score is None:
        #     baseline_obj.score = score
        #     self.write_knowledge(baseline_obj)
        #
        # ratio_list_obj = self.read_knowledge(AnomalyRatioList)
        # ratio_list_obj.ratio_list.append(score / baseline_obj.score)
        # self.write_knowledge(ratio_list_obj)
        #
        # obs_list_obj = self.read_knowledge(ObservationList)
        # obs_list_obj.observations.append(observation)
        # self.write_knowledge(obs_list_obj)
        #
        # in_planning_obj = self.read_knowledge(InPlanning)
        # if not in_planning_obj.in_planning:
        #     self.publish_event(event_key='observation_recorded')
        pass

    def register_callbacks(self):
        # TODO: replace 'sensor_data_received' with your incoming event / topic name
        self.register_event_callback(event_key='sensor_data_received', callback=self.monitor)


class Analysis(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Analysis"
        self.logger.info("Analysis instantiated")

    @timeit_callback
    def analysis(self, msg):
        """
        Performs anomaly detection on the latest observations.

        Steps to implement:
          1. Read AnomalyRatioList from knowledge.
          2. Apply a sliding detection window (last N samples).
          3. If all samples in the window exceed a threshold:
               - Write InPlanning = True to knowledge.
               - Write AbnormalStartIdx to knowledge.
               - Publish 'anomaly' to trigger Plan.
        """
        # TODO: implement anomaly detection logic, e.g.:
        #
        # detect_window_size = 5
        # threshold = 2.0
        #
        # ratio_list = self.read_knowledge(AnomalyRatioList).ratio_list
        # if len(ratio_list) > detect_window_size:
        #     window = ratio_list[-detect_window_size:]
        #     if all(r > threshold for r in window):
        #         _in_planning = InPlanning()
        #         _in_planning.in_planning = True
        #         self.write_knowledge(_in_planning)
        #
        #         _abnormal_start_idx = AbnormalStartIdx()
        #         _abnormal_start_idx.abnormal_start_idx = len(ratio_list) - detect_window_size
        #         self.write_knowledge(_abnormal_start_idx)
        #
        #         self.publish_event(event_key='anomaly_detected')
        pass

    def register_callbacks(self):
        self.register_event_callback(event_key='observation_recorded', callback=self.analysis)


class Plan(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Plan"
        self.logger.info("Plan instantiated")


    @timeit_callback
    def planner(self, msg):
        """
        Generates a new plan / model adaptation in a background thread.

        Steps to implement:
          1. Read AbnormalStartIdx, ReplanningCounter, and ObservationList from knowledge.
          2. Wait (polling with sleep) until enough new observations have accumulated
             after the anomaly index.
          3. Use accumulated data to generate a new plan or retrain a model.
          4. Publish 'new_plan' to trigger Legitimate.
        """
        # TODO: implement planning / model retraining logic, e.g.:
        #
        # abnormal_start_idx = self.read_knowledge(AbnormalStartIdx).abnormal_start_idx
        # replanning_counter = self.read_knowledge(ReplanningCounter).replanning_counter
        # data_needed = <compute_needed_samples>(replanning_counter)
        # retraining_complete = False
        #
        # while not retraining_complete:
        #     obs_list = self.read_knowledge(ObservationList).observations
        #     if len(obs_list) >= abnormal_start_idx + data_needed:
        #         training_data = obs_list[abnormal_start_idx : abnormal_start_idx + data_needed]
        #         <retrain_or_adapt>(training_data)
        #         retraining_complete = True
        #     else:
        #         time.sleep(1)
        #
        # self.publish_event(event_key='plan_generated')
        pass

    def register_callbacks(self):
        self.register_event_callback(event_key='anomaly_detected', callback=self.planner)
        self.register_event_callback(event_key='plan_rejected', callback=self.planner)


class Legitimate(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Legitimate"
        self.logger.info("Legitimate instantiated")

    @timeit_callback
    def legitimizer(self, msg):
        """
        Validates the newly generated plan / model before execution.

        Steps to implement:
          1. Read the new plan / model and recent observations from knowledge.
          2. Score the new plan against a validation window.
          3. If validation passes:
               - Reset ReplanningCounter to 0 and AbnormalStartIdx to None.
               - Publish 'isLegit' to trigger Execute.
          4. If validation fails:
               - Increment ReplanningCounter.
               - Clean up the invalid plan.
               - Publish 'anomaly' to trigger re-planning.
        """
        # TODO: implement legitimization / validation logic, e.g.:
        #
        # threshold = 4.0
        # detect_window_size = 5
        # obs_list = self.read_knowledge(ObservationList).observations
        # baseline = self.read_knowledge(BaselineScore).score
        # replanning_counter = self.read_knowledge(ReplanningCounter).replanning_counter
        #
        # validation_scores = [<score_new_plan>(obs) for obs in obs_list[-detect_window_size:]]
        # ratios = [s / baseline for s in validation_scores]
        #
        # if any(r > threshold for r in ratios):
        #     _rc = ReplanningCounter()
        #     _rc.replanning_counter = replanning_counter + 1
        #     self.write_knowledge(_rc)
        #     self.publish_event(event_key='plan_rejected')
        # else:
        #     _rc = ReplanningCounter()
        #     _rc.replanning_counter = 0
        #     self.write_knowledge(_rc)
        #     _asi = AbnormalStartIdx()
        #     _asi.abnormal_start_idx = None
        #     self.write_knowledge(_asi)
        #     self.publish_event(event_key='plan_validated')
        pass

    def register_callbacks(self):
        self.register_event_callback(event_key='plan_generated', callback=self.legitimizer)


class Execute(Node):

    def __init__(self, config='config.yaml', verbose=True):
        super().__init__(config=config, verbose=verbose)
        self._name = "Execute"
        self.logger.info("Execute instantiated")

    @timeit_callback
    def executer(self, msg):
        """
        Applies the validated plan / model to the live system.

        Steps to implement:
          1. Read the validated plan / model from knowledge or disk.
          2. Deploy it to the target system (e.g., swap model files, update parameters).
          3. Reset InPlanning to False so monitoring resumes normally.
          4. Publish 'plan_executed' to signal completion downstream.
        """
        # TODO: implement execution logic, e.g.:
        #
        # <apply_new_plan_or_model>()
        #
        # _in_planning = InPlanning()
        # _in_planning.in_planning = False
        # self.write_knowledge(_in_planning)
        #
        # self.publish_event(event_key='plan_executed')
        pass

    def register_callbacks(self):
        self.register_event_callback(event_key='plan_validated', callback=self.executer)


def main(args=None):
    try:
        with open('mape_config.yaml', 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        raise Exception("Config file 'mape_config.yaml' not found")

    monitor_node = Monitor(config['Monitor_Config'])
    monitor_node.register_callbacks()
    monitor_node.start()

    analysis_node = Analysis(config['Analysis_Config'])
    analysis_node.register_callbacks()
    analysis_node.start()

    plan_node = Plan(config['Plan_Config'])
    plan_node.register_callbacks()
    plan_node.start()

    legitimate_node = Legitimate(config['Legitimate_Config'])
    legitimate_node.register_callbacks()
    legitimate_node.start()

    execute_node = Execute(config['Execute_Config'])
    execute_node.register_callbacks()
    execute_node.start()

    # The dashboard's Trustworthiness Properties panel is generic: point
    # tc_spec_file at your use case's .dsrv and everything else is derived
    # from it - every `out <name>: Bool` becomes a row, the trailing `// ...`
    # comment on the declaration is its display label, and declarations
    # sharing a comment are grouped into one row. The TC checker must run
    # with --redis-output for the panel to receive values.
    # TODO: point tc_spec_file at your spec, or drop the kwarg to hide the panel.
    t = threading.Thread(
        target=run_dashboard,
        kwargs={'host': '127.0.0.1', 'port': 8050, 'debug': False, 'start_trust': True,
                # 'tc_spec_file': 'path/to/your_spec.dsrv',
                },
        daemon=True
    )
    t.start()


if __name__ == '__main__':
    main()
    try:
        while True:
            time.sleep(1)
    except:
        exit()
