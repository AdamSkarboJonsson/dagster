import logging
import os
from abc import ABCMeta
from collections import defaultdict, namedtuple
from enum import Enum

import six
import yaml
from rx import Observable

from dagster import check, seven
from dagster.config import Field, Permissive
from dagster.core.definitions.pipeline import PipelineRunsFilter
from dagster.core.errors import (
    DagsterInvalidConfigError,
    DagsterInvariantViolationError,
    DagsterRunAlreadyExists,
    DagsterRunConflict,
)
from dagster.core.serdes import ConfigurableClass, whitelist_for_serdes
from dagster.core.storage.pipeline_run import PipelineRun
from dagster.utils.yaml_utils import load_yaml_from_globs

from .config import DAGSTER_CONFIG_YAML_FILENAME
from .ref import InstanceRef, compute_logs_directory


def _is_dagster_home_set():
    return bool(os.getenv('DAGSTER_HOME'))


def _dagster_home():
    dagster_home_path = os.getenv('DAGSTER_HOME')

    if not dagster_home_path:
        raise DagsterInvariantViolationError(
            'DAGSTER_HOME is not set, check is_dagster_home_set before invoking.'
        )

    return os.path.expanduser(dagster_home_path)


class _EventListenerLogHandler(logging.Handler):
    def __init__(self, instance):
        self._instance = instance
        super(_EventListenerLogHandler, self).__init__()

    def emit(self, record):
        from dagster.core.events.log import construct_event_record, StructuredLoggerMessage

        try:
            event = construct_event_record(
                StructuredLoggerMessage(
                    name=record.name,
                    message=record.msg,
                    level=record.levelno,
                    meta=record.dagster_meta,
                    record=record,
                )
            )

            self._instance.handle_new_event(event)

        except Exception as e:  # pylint: disable=W0703
            logging.critical('Error during instance event listen')
            logging.exception(str(e))
            raise


class InstanceType(Enum):
    PERSISTENT = 'PERSISTENT'
    EPHEMERAL = 'EPHEMERAL'


class DagsterInstance:
    '''Core abstraction for managing Dagster's access to storage and other resources.

    Users should not directly instantiate this class; it is instantiated by internal machinery when
    ``dagit`` and ``dagster-graphql`` load, based on the values in the ``dagster.yaml`` file in
    ``$DAGSTER_HOME``. Configuration of this class should be done by setting values in that file.
    For example, to use Postgres for run and event log storage, you can write a ``dagster.yaml``
    such as the following:

    .. literalinclude:: ../../../../docs/sections/deploying/dagster.yaml
       :caption: dagster.yaml

    Args:
        instance_type (InstanceType): Indicates whether the instance is ephemeral or persistent.
            Users should not attempt to set this value directly or in their ``dagster.yaml`` files.
        local_artifact_storage (LocalArtifactStorage): The local artifact storage is used to
            configure storage for any artifacts that require a local disk, such as schedules, or
            when using the fileystem system storage to manage files and intermediates. By default,
            this will be a :py:class:`dagster.core.storage.root.LocalArtifactStorage`. Configurable
            in ``dagster.yaml`` using the :py:class:`~dagster.core.serdes.ConfigurableClass`
            machinery.
        run_storage (RunStorage): The run storage is used to store metadata about ongoing and past
            pipeline runs. By default, this will be a
            :py:class:`dagster.core.storage.runs.SqliteRunStorage`. Configurable in ``dagster.yaml``
            using the :py:class:`~dagster.core.serdes.ConfigurableClass` machinery.
        event_storage (EventLogStorage): Used to store the structured event logs generated by
            pipeline runs. By default, this will be a
            :py:class:`dagster.core.storage.event_log.SqliteEventLogStorage`. Configurable in
            ``dagster.yaml`` using the :py:class:`~dagster.core.serdes.ConfigurableClass` machinery.
        compute_log_manager (ComputeLogManager): The compute log manager handles stdout and stderr
            logging for solid compute functions. By default, this will be a
            :py:class:`dagster.core.storage.local_compute_log_manager.LocalComputeLogManager`. 
            Configurable in ``dagster.yaml`` using the
            :py:class:`~dagster.core.serdes.ConfigurableClass` machinery.
        run_launcher (Optional[RunLauncher]): Optionally, a run launcher may be used to enable
            a Dagster instance to launch pipeline runs, e.g. on a remote Kubernetes cluster, in
            addition to runnning them locally.
        dagit_settings (Optional[Dict]): Specifies certain Dagit-specific, per-instance settings,
            such as feature flags. These are set in the ``dagster.yaml`` under the key ``dagit``.
        ref (Optional[InstanceRef]): Used by internal machinery to pass instances across process
            boundaries.
    '''

    _PROCESS_TEMPDIR = None

    def __init__(
        self,
        instance_type,
        local_artifact_storage,
        run_storage,
        event_storage,
        compute_log_manager,
        run_launcher=None,
        dagit_settings=None,
        ref=None,
    ):
        from dagster.core.storage.compute_log_manager import ComputeLogManager
        from dagster.core.storage.event_log import EventLogStorage
        from dagster.core.storage.root import LocalArtifactStorage
        from dagster.core.storage.runs import RunStorage
        from dagster.core.launcher import RunLauncher

        self._instance_type = check.inst_param(instance_type, 'instance_type', InstanceType)
        self._local_artifact_storage = check.inst_param(
            local_artifact_storage, 'local_artifact_storage', LocalArtifactStorage
        )
        self._event_storage = check.inst_param(event_storage, 'event_storage', EventLogStorage)
        self._run_storage = check.inst_param(run_storage, 'run_storage', RunStorage)
        self._compute_log_manager = check.inst_param(
            compute_log_manager, 'compute_log_manager', ComputeLogManager
        )
        self._run_launcher = check.opt_inst_param(run_launcher, 'run_launcher', RunLauncher)
        self._dagit_settings = check.opt_dict_param(dagit_settings, 'dagit_settings')
        self._ref = check.opt_inst_param(ref, 'ref', InstanceRef)

        self._subscribers = defaultdict(list)

    # ctors

    @staticmethod
    def ephemeral(tempdir=None):
        from dagster.core.storage.event_log import InMemoryEventLogStorage
        from dagster.core.storage.root import LocalArtifactStorage
        from dagster.core.storage.runs import InMemoryRunStorage
        from dagster.core.storage.local_compute_log_manager import NoOpComputeLogManager

        if tempdir is None:
            tempdir = DagsterInstance.temp_storage()

        return DagsterInstance(
            InstanceType.EPHEMERAL,
            local_artifact_storage=LocalArtifactStorage(tempdir),
            run_storage=InMemoryRunStorage(),
            event_storage=InMemoryEventLogStorage(),
            compute_log_manager=NoOpComputeLogManager(compute_logs_directory(tempdir)),
        )

    @staticmethod
    def get(fallback_storage=None):
        # 1. Use $DAGSTER_HOME to determine instance if set.
        if _is_dagster_home_set():
            return DagsterInstance.from_config(_dagster_home())

        # 2. If that is not set use the fallback storage directory if provided.
        # This allows us to have a nice out of the box dagit experience where runs are persisted
        # across restarts in a tempdir that gets cleaned up when the dagit watchdog process exits.
        elif fallback_storage is not None:
            return DagsterInstance.from_config(fallback_storage)

        # 3. If all else fails create an ephemeral in memory instance.
        else:
            return DagsterInstance.ephemeral(fallback_storage)

    @staticmethod
    def local_temp(tempdir=None, overrides=None):
        if tempdir is None:
            tempdir = DagsterInstance.temp_storage()

        return DagsterInstance.from_ref(InstanceRef.from_dir(tempdir, overrides=overrides))

    @staticmethod
    def from_config(config_dir, config_filename=DAGSTER_CONFIG_YAML_FILENAME):
        instance_ref = InstanceRef.from_dir(config_dir, config_filename=config_filename)
        return DagsterInstance.from_ref(instance_ref)

    @staticmethod
    def from_ref(instance_ref):
        check.inst_param(instance_ref, 'instance_ref', InstanceRef)
        return DagsterInstance(
            instance_type=InstanceType.PERSISTENT,
            local_artifact_storage=instance_ref.local_artifact_storage,
            run_storage=instance_ref.run_storage,
            event_storage=instance_ref.event_storage,
            compute_log_manager=instance_ref.compute_log_manager,
            run_launcher=instance_ref.run_launcher,
            dagit_settings=instance_ref.dagit_settings,
            ref=instance_ref,
        )

    # flags

    @property
    def is_persistent(self):
        return self._instance_type == InstanceType.PERSISTENT

    @property
    def is_ephemeral(self):
        return self._instance_type == InstanceType.EPHEMERAL

    def get_ref(self):
        if self._ref:
            return self._ref

        check.failed('Can not produce an instance reference for {t}'.format(t=self))

    @property
    def root_directory(self):
        return self._local_artifact_storage.base_dir

    @staticmethod
    def temp_storage():
        if DagsterInstance._PROCESS_TEMPDIR is None:
            DagsterInstance._PROCESS_TEMPDIR = seven.TemporaryDirectory()
        return DagsterInstance._PROCESS_TEMPDIR.name

    def info_str(self):
        def _info(component):
            prefix = '     '
            if isinstance(component, ConfigurableClass):
                return component.inst_data.info_str(prefix)
            if type(component) is dict:
                return prefix + yaml.dump(component, default_flow_style=False).replace(
                    '\n', '\n' + prefix
                )
            return '{}{}\n'.format(prefix, component.__class__.__name__)

        dagit_settings = self._dagit_settings if self._dagit_settings else None

        return (
            'DagsterInstance components:\n\n'
            '  Local Artifacts Storage:\n{artifact}\n'
            '  Run Storage:\n{run}\n'
            '  Event Log Storage:\n{event}\n'
            '  Compute Log Manager:\n{compute}\n'
            '  Run Launcher:\n{run_launcher}\n'
            '  Dagit:\n{dagit}\n'
            ''.format(
                artifact=_info(self._local_artifact_storage),
                run=_info(self._run_storage),
                event=_info(self._event_storage),
                compute=_info(self._compute_log_manager),
                run_launcher=_info(self._run_launcher),
                dagit=_info(dagit_settings),
            )
        )

    # run launcher

    @property
    def run_launcher(self):
        return self._run_launcher

    # compute logs

    @property
    def compute_log_manager(self):
        return self._compute_log_manager

    @property
    def dagit_settings(self):
        if self._dagit_settings:
            return self._dagit_settings
        return {}

    def upgrade(self, print_fn=lambda _: None):
        print_fn('Updating run storage...')
        self._run_storage.upgrade()

        print_fn('Updating event storage...')
        self._event_storage.upgrade()

    def dispose(self):
        self._run_storage.dispose()
        self._event_storage.dispose()

    # run storage

    def get_run_by_id(self, run_id):
        return self._run_storage.get_run_by_id(run_id)

    def get_run_stats(self, run_id):
        return self._event_storage.get_stats_for_run(run_id)

    def get_run_tags(self):
        return self._run_storage.get_run_tags()

    def create_empty_run(self, run_id, pipeline_name):
        return self.create_run(PipelineRun.create_empty_run(pipeline_name, run_id))

    def create_run(self, pipeline_run):
        check.inst_param(pipeline_run, 'pipeline_run', PipelineRun)

        if self.has_run(pipeline_run.run_id):
            raise DagsterRunAlreadyExists(
                'Attempting to create a pipeline run for an existing run id, {run_id}'.format(
                    run_id=pipeline_run.run_id
                )
            )

        run = self._run_storage.add_run(pipeline_run)
        return run

    def get_or_create_run(self, pipeline_run):
        # This eventually needs transactional/locking semantics
        if self.has_run(pipeline_run.run_id):
            candidate_run = self.get_run_by_id(pipeline_run.run_id)
            if not candidate_run == pipeline_run:
                raise DagsterRunConflict(
                    'Found conflicting existing run with same id. Expected {pipeline_run}, found {candidate_run}.'.format(
                        pipeline_run=pipeline_run, candidate_run=candidate_run
                    )
                )
            return candidate_run
        else:
            # We will need a more principled way of doing this
            try:
                return self.create_run(pipeline_run)
            except DagsterRunAlreadyExists:
                if not self.has_run(pipeline_run.run_id):
                    check.failed(
                        'Inconsistent run storage: could not get or create pipeline run with run_id {run_id}'.format(
                            run_id=pipeline_run.run_id
                        )
                    )

                return self.get_run_by_id(pipeline_run.run_id)

    def add_run(self, pipeline_run):
        return self._run_storage.add_run(pipeline_run)

    def handle_run_event(self, run_id, event):
        return self._run_storage.handle_run_event(run_id, event)

    def has_run(self, run_id):
        return self._run_storage.has_run(run_id)

    def get_runs(self, filters=None, cursor=None, limit=None):
        return self._run_storage.get_runs(filters, cursor, limit)

    def get_runs_count(self, filters=None):
        return self._run_storage.get_runs_count(filters)

    def wipe(self):
        self._run_storage.wipe()
        self._event_storage.wipe()

    def delete_run(self, run_id):
        self._run_storage.delete_run(run_id)
        self._event_storage.delete_events(run_id)

    # event storage

    def logs_after(self, run_id, cursor):
        return self._event_storage.get_logs_for_run(run_id, cursor=cursor)

    def all_logs(self, run_id):
        return self._event_storage.get_logs_for_run(run_id)

    def watch_event_logs(self, run_id, cursor, cb):
        return self._event_storage.watch(run_id, cursor, cb)

    # event subscriptions

    def get_logger(self):
        logger = logging.Logger('__event_listener')
        logger.addHandler(_EventListenerLogHandler(self))
        logger.setLevel(10)
        return logger

    def handle_new_event(self, event):
        run_id = event.run_id

        self._event_storage.store_event(event)

        if event.is_dagster_event and event.dagster_event.is_pipeline_event:
            self._run_storage.handle_run_event(run_id, event.dagster_event)

        for sub in self._subscribers[run_id]:
            sub(event)

    def add_event_listener(self, run_id, cb):
        self._subscribers[run_id].append(cb)

    # directories

    def file_manager_directory(self, run_id):
        return self._local_artifact_storage.file_manager_dir(run_id)

    def intermediates_directory(self, run_id):
        return self._local_artifact_storage.intermediates_dir(run_id)

    def schedules_directory(self):
        return self._local_artifact_storage.schedules_dir

    # Run launcher

    def launch_run(self, run):
        return self._run_launcher.launch_run(self, run)
