#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# pytype: skip-file

import asyncio
import logging
import os
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
import uuid
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any
from typing import Optional

from openai import AsyncOpenAI
from openai import OpenAI

from apache_beam.io.filesystems import FileSystems
from apache_beam.metrics.metric import Metrics
from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.utils import subprocess_server

try:
  # VLLM logging config breaks beam logging.
  os.environ["VLLM_CONFIGURE_LOGGING"] = "0"
  import vllm  # pylint: disable=unused-import
  logging.info('vllm module successfully imported.')
  os.environ["VLLM_CONFIGURE_LOGGING"] = "1"
except ModuleNotFoundError:
  msg = 'vllm module was not found. This is ok as long as the specified ' \
    'runner has vllm dependencies installed.'
  logging.warning(msg)

__all__ = [
    'OpenAIChatMessage',
    'VLLMCompletionsModelHandler',
    'VLLMChatModelHandler',
]

# --- Autoscaling support (inlined for POC) ---------------------------------
# Dataflow's horizontal autoscaler scales GPU/model pools on the process-wide
# metric "num_loaded_models_<tag>" in the "BeamML_ModelManager" namespace (see
# model_manager.py and
# //dist_proc/windmill/autoscaling/horizontal_scaling_policy.cc). A single vLLM
# server per worker always has 1 loaded copy, so instead we publish that same
# metric with a value derived from vLLM's live request concurrency.
_MODEL_MANAGER_METRICS_NAMESPACE = 'BeamML_ModelManager'
_NUM_LOADED_MODELS_METRIC_PREFIX = 'num_loaded_models_'


def _parse_prometheus_labels(label_str: str) -> dict[str, str]:
  """Parses the ``{k="v",...}`` label section of a Prometheus sample line."""
  labels: dict[str, str] = {}
  i = 0
  n = len(label_str)
  while i < n:
    eq = label_str.find('=', i)
    if eq == -1:
      break
    name = label_str[i:eq].strip()
    j = eq + 1
    if j < n and label_str[j] == '"':
      j += 1
      value_chars = []
      while j < n and label_str[j] != '"':
        if label_str[j] == '\\' and j + 1 < n:
          j += 1
        value_chars.append(label_str[j])
        j += 1
      value = ''.join(value_chars)
      j += 1  # skip closing quote
    else:
      comma = label_str.find(',', j)
      comma = n if comma == -1 else comma
      value = label_str[j:comma].strip()
      j = comma
    labels[name] = value
    comma = label_str.find(',', j)
    if comma == -1:
      break
    i = comma + 1
  return labels


def parse_prometheus_metric(
    metrics_text: str,
    metric_name: str,
    label_filter: Optional[Mapping[str, str]] = None) -> Optional[float]:
  """Sums the values of a metric from Prometheus text-format exposition.

  Args:
    metrics_text: The full body returned by the server's ``/metrics`` endpoint.
    metric_name: The metric family name to match (e.g.
      ``vllm:num_requests_running``).
    label_filter: If provided, only samples whose labels contain all of these
      key/value pairs are summed.

  Returns:
    The sum of the matching sample values, or ``None`` if the metric is absent.
  """
  total: Optional[float] = None
  for raw_line in metrics_text.splitlines():
    line = raw_line.strip()
    if not line or line.startswith('#'):
      continue
    if not line.startswith(metric_name):
      continue
    remainder = line[len(metric_name):]
    if remainder and remainder[0] not in (' ', '\t', '{'):
      continue
    labels: Mapping[str, str] = {}
    if remainder.startswith('{'):
      close = remainder.find('}')
      if close == -1:
        continue
      labels = _parse_prometheus_labels(remainder[1:close])
      remainder = remainder[close + 1:]
    parts = remainder.split()
    if not parts:
      continue
    try:
      value = float(parts[0])
    except ValueError:
      continue
    if label_filter and any(labels.get(k) != v
                            for k, v in label_filter.items()):
      continue
    total = value if total is None else total + value
  return total


class NumLoadedModelsPublisher:
  """Publishes the ``num_loaded_models_<tag>`` autoscaling metric on a timer.

  ``signal_fn`` produces the raw load sample (e.g. requests running in
  parallel); it is polled on a background daemon thread every
  ``poll_interval_secs``. Returning ``None`` (e.g. a transient scrape failure)
  skips that tick, leaving the running state and last published value unchanged.

  The published value is an aggregate of the observed load, chosen so the pool
  does not scale *down* the instant an input burst clears (which would thrash
  and then be unable to absorb the next burst):

    * ``'lifetime_max'`` -- the maximum concurrency observed over the whole
                     pipeline lifetime (monotonic; never decays). Sizes the
                     fleet to the worst load ever seen and holds it. Default.
    * ``'max'``   -- peak concurrency in the trailing
                     ``aggregation_window_secs`` window; decays that long after
                     load drops.
    * ``'p<NN>'`` -- a windowed percentile, e.g. ``'p90'``. Smoother than max:
                     ignores brief spikes and decays as low samples fill it.
    * ``'mean'``  -- the average over the window.
    * ``'last'``  -- the raw last sample (no smoothing; original behavior).
  """
  def __init__(
      self,
      tag: str,
      signal_fn: Callable[[], Optional[float]],
      poll_interval_secs: float = 5.0,
      aggregation: str = 'lifetime_max',
      aggregation_window_secs: float = 60.0):
    if not tag:
      raise ValueError('num_loaded_models tag must be a non-empty string.')
    self._metric_name = _NUM_LOADED_MODELS_METRIC_PREFIX + tag
    self._signal_fn = signal_fn
    self._poll_interval_secs = poll_interval_secs
    self._aggregation = (aggregation or 'lifetime_max').lower()
    self._aggregation_window_secs = max(float(aggregation_window_secs), 0.0)
    # Trailing (timestamp, value) samples for the windowed aggregation modes.
    self._samples: list[tuple[float, float]] = []
    # Monotonic peak for the 'lifetime_max' mode (no sample retention).
    self._lifetime_max: Optional[float] = None
    self._stop_event = threading.Event()
    self._thread: Optional[threading.Thread] = None

  def _publish(self, value: float) -> None:
    Metrics.distribution(
        _MODEL_MANAGER_METRICS_NAMESPACE, self._metric_name,
        process_wide=True).update(int(round(value)))

  def _aggregate(self, now: float) -> Optional[float]:
    """Aggregates in-window samples per ``self._aggregation``.

    Evicts samples older than ``aggregation_window_secs`` first, then reduces
    the remainder.

    Args:
      now: Current timestamp used for window eviction.

    Returns:
      The aggregated value, or ``None`` if the window is empty.
    """
    if self._aggregation_window_secs > 0:
      cutoff = now - self._aggregation_window_secs
      self._samples = [(t, v) for (t, v) in self._samples if t >= cutoff]
    values = [v for (_, v) in self._samples]
    if not values:
      return None
    mode = self._aggregation
    if mode == 'last':
      return values[-1]
    if mode == 'mean':
      return sum(values) / len(values)
    if mode.startswith('p'):
      try:
        q = min(max(int(mode[1:]), 0), 100)
      except ValueError:
        return max(values)
      ordered = sorted(values)
      # Nearest-rank percentile: rank = ceil(q/100 * n).
      rank = (q * len(ordered) + 99) // 100
      idx = min(max(rank - 1, 0), len(ordered) - 1)
      return ordered[idx]
    return max(values)  # 'max' and unknown modes.

  def _run(self) -> None:
    while not self._stop_event.is_set():
      try:
        value = self._signal_fn()
        if value is not None:
          if self._aggregation == 'lifetime_max':
            # Monotonic running max over the whole pipeline lifetime; no
            # per-sample retention and never decays.
            self._lifetime_max = (
                float(value) if self._lifetime_max is None else max(
                    self._lifetime_max, float(value)))
            aggregate = self._lifetime_max
          else:
            now = time.time()
            self._samples.append((now, float(value)))
            aggregate = self._aggregate(now)
          if aggregate is not None:
            self._publish(aggregate)
      except Exception:  # pylint: disable=broad-except
        logging.exception(
            'Failed to publish autoscaling metric %s; will retry.',
            self._metric_name)
      self._stop_event.wait(self._poll_interval_secs)

  def start(self) -> None:
    """Publishes an initial 0 and starts the background polling thread."""
    if self._thread is not None:
      return
    try:
      self._publish(0)
    except Exception:  # pylint: disable=broad-except
      logging.exception(
          'Failed to publish initial autoscaling metric %s.', self._metric_name)
    self._thread = threading.Thread(
        target=self._run, name='num-loaded-models-publisher', daemon=True)
    self._thread.start()

  def stop(self) -> None:
    """Stops polling and publishes a final 0 so a drained worker sheds load."""
    self._stop_event.set()
    thread = self._thread
    if thread is not None:
      thread.join(timeout=self._poll_interval_secs + 5)
      self._thread = None
    try:
      self._publish(0)
    except Exception:  # pylint: disable=broad-except
      pass


@dataclass(frozen=True)
class OpenAIChatMessage():
  """"
  Dataclass containing previous chat messages in conversation.
  Role is the entity that sent the message (either 'user' or 'system').
  Content is the contents of the message.
  """
  role: str
  content: str


def start_process(cmd) -> tuple[subprocess.Popen, int]:
  port, = subprocess_server.pick_port(None)
  cmd = [arg.replace('{{PORT}}', str(port)) for arg in cmd]  # pylint: disable=not-an-iterable
  logging.info("Starting service with %s", str(cmd).replace("',", "'"))
  process = subprocess.Popen(
      cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

  # Emit the output of this command as info level logging.
  def log_stdout():
    line = process.stdout.readline()
    while line:
      # The log obtained from stdout is bytes, decode it into string.
      # Remove newline via rstrip() to not print an empty line.
      logging.info(line.decode(errors='backslashreplace').rstrip())
      line = process.stdout.readline()

  t = threading.Thread(target=log_stdout)
  t.daemon = True
  t.start()
  return process, port


def getVLLMClient(port) -> OpenAI:
  openai_api_key = "EMPTY"
  openai_api_base = f"http://localhost:{port}/v1"
  return OpenAI(
      api_key=openai_api_key,
      base_url=openai_api_base,
  )


def getAsyncVLLMClient(port) -> AsyncOpenAI:
  openai_api_key = "EMPTY"
  openai_api_base = f"http://localhost:{port}/v1"
  return AsyncOpenAI(
      api_key=openai_api_key,
      base_url=openai_api_base,
  )


_VLLM_DEFAULT_AUTOSCALING_POLL_SECS = 5.0
_VLLM_DEFAULT_AUTOSCALING_AGGREGATION = 'lifetime_max'
_VLLM_DEFAULT_AUTOSCALING_WINDOW_SECS = 60.0


def _sanitize_metric_tag(name: str) -> str:
  """Turns a model name into a metric-tag-safe token for the autoscaler.

  Args:
    name: The model name (may contain ``/``, ``:`` etc.).

  Returns:
    ``name`` with characters outside ``[A-Za-z0-9_]`` replaced by ``_``.
  """
  return ''.join(c if (c.isalnum() or c == '_') else '_' for c in name)


def _scrape_vllm_load_signal(
    port: int,
    include_waiting: bool = True,
    timeout_secs: float = 5.0) -> Optional[float]:
  """Reads vLLM's live request concurrency from its Prometheus endpoint.

  This is the default autoscaling signal for the vLLM handlers: the number of
  requests the engine is currently running, optionally plus those queued
  waiting. It is published under ``num_loaded_models_<tag>`` so Dataflow's
  autoscaler scales on real serving load rather than the (always 1) count of
  loaded vLLM servers.

  Args:
    port: Local port of the vLLM OpenAI-compatible server.
    include_waiting: Whether to add ``vllm:num_requests_waiting`` to the
      running count.
    timeout_secs: Per-scrape HTTP timeout in seconds.

  Returns:
    The concurrency value, or ``None`` if the metric could not be scraped (e.g.
    the server is momentarily unavailable), which the publisher treats as
    "skip this tick".
  """
  url = f'http://localhost:{port}/metrics'
  try:
    with urllib.request.urlopen(url, timeout=timeout_secs) as response:
      text = response.read().decode('utf-8', errors='replace')
  except Exception:  # pylint: disable=broad-except
    return None
  running = parse_prometheus_metric(text, 'vllm:num_requests_running')
  if running is None:
    return None
  total = running
  if include_waiting:
    waiting = parse_prometheus_metric(text, 'vllm:num_requests_waiting')
    if waiting is not None:
      total += waiting
  return total


# Embedded Dynamo runtime defaults proven on the smoke test: etcd discovery,
# TCP request plane, ZMQ event plane, KV events disabled. KV-aware routing,
# disaggregated prefill/decode, and the Planner are not active in this mode.
_DYNAMO_FRONTEND_DEFAULT_KWARGS: dict[str, Optional[str]] = {
    'discovery-backend': 'etcd',
    'request-plane': 'tcp',
    'event-plane': 'zmq',
    'router-mode': 'round-robin',
    'no-router-kv-events': None,
}

_DYNAMO_ENGINE_DEFAULT_KWARGS: dict[str, Optional[str]] = {
    'discovery-backend': 'etcd',
    'request-plane': 'tcp',
    'event-plane': 'zmq',
    'kv-events-config': '{"enable_kv_cache_events": false}',
}


def _append_kwargs(cmd: list[str], kwargs: dict[str, Optional[str]]) -> None:
  for k, v in kwargs.items():
    cmd.append(f'--{k}')
    # Only add values for commands with value part.
    if v is not None:
      cmd.append(v)


def _uses_etcd_discovery(kwargs: dict[str, Optional[str]]) -> bool:
  return kwargs.get('discovery-backend') == 'etcd'


class _VLLMModelServer():
  def __init__(
      self,
      model_name: str,
      vllm_server_kwargs: dict[str, Optional[str]],
      dynamo_frontend_kwargs: Optional[dict[str, Optional[str]]] = None,
      use_dynamo: bool = False,
      autoscaling_tag: Optional[str] = None,
      autoscaling_poll_interval_secs:
      float = _VLLM_DEFAULT_AUTOSCALING_POLL_SECS,
      autoscaling_include_waiting: bool = False,
      autoscaling_aggregation: str = _VLLM_DEFAULT_AUTOSCALING_AGGREGATION,
      autoscaling_aggregation_window_secs: float = (
          _VLLM_DEFAULT_AUTOSCALING_WINDOW_SECS),
      load_signal_fn: Optional[Callable[[int], Optional[float]]] = None):
    self._model_name = model_name
    self._vllm_server_kwargs = vllm_server_kwargs
    self._dynamo_frontend_kwargs = dynamo_frontend_kwargs or {}
    self._server_started = False
    self._server_process = None
    self._dynamo_process = None
    self._etcd_process = None
    self._etcd_data_dir: Optional[str] = None
    self._managed_etcd_endpoint = None
    self._server_port: int = -1
    self._server_process_lock = threading.RLock()
    self._use_dynamo = use_dynamo
    self._autoscaling_include_waiting = autoscaling_include_waiting
    self._autoscaling_aggregation = autoscaling_aggregation
    self._autoscaling_aggregation_window_secs = (
        autoscaling_aggregation_window_secs)
    self._load_signal_fn = load_signal_fn
    # Publishes the num_loaded_models_<tag> autoscaling signal derived from the
    # vLLM server's live request-concurrency metrics. Started once the server
    # is confirmed up (see start_server). None disables the metric entirely.
    self._num_loaded_models_publisher: Optional[NumLoadedModelsPublisher] = (
        NumLoadedModelsPublisher(
            tag=autoscaling_tag,
            signal_fn=self._get_load_signal,
            poll_interval_secs=autoscaling_poll_interval_secs,
            aggregation=autoscaling_aggregation,
            aggregation_window_secs=autoscaling_aggregation_window_secs)
        if autoscaling_tag else None)

    self.start_server()

  def _get_load_signal(self) -> Optional[float]:
    """Returns the current load-derived num_loaded_models value, or None.

    Returns None while the server is down/restarting so the publisher skips the
    tick instead of reporting a misleading 0.
    """
    if not self._server_started or self._server_port < 0:
      return None
    if self._load_signal_fn is not None:
      return self._load_signal_fn(self._server_port)
    return _scrape_vllm_load_signal(
        self._server_port, include_waiting=self._autoscaling_include_waiting)

  @staticmethod
  def _stop_process(process: Optional[subprocess.Popen]) -> None:
    if process is None or process.poll() is not None:
      return
    # A process may exit between poll() and terminate() / kill(), in which
    # case the OS raises ProcessLookupError (or another OSError). Treat that
    # as already-stopped so we don't bail out of the broader cleanup.
    try:
      process.terminate()
      try:
        process.wait(timeout=10)
      except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    except OSError:
      pass

  def _stop_processes(self) -> None:
    self._stop_process(self._dynamo_process)
    self._stop_process(self._server_process)
    self._stop_process(self._etcd_process)
    if (self._managed_etcd_endpoint is not None and
        os.environ.get('ETCD_ENDPOINTS') == self._managed_etcd_endpoint):
      os.environ.pop('ETCD_ENDPOINTS', None)
    if self._etcd_data_dir is not None:
      shutil.rmtree(self._etcd_data_dir, ignore_errors=True)
      self._etcd_data_dir = None
    self._dynamo_process = None
    self._server_process = None
    self._etcd_process = None
    self._managed_etcd_endpoint = None
    self._server_started = False
    self._server_port = -1

  def _process_status(self) -> str:
    process_status = []
    if self._server_process is not None:
      process_status.append(
          'frontend/server exit code: %s' % self._server_process.poll())
    if self._dynamo_process is not None:
      process_status.append(
          'dynamo worker exit code: %s' % self._dynamo_process.poll())
    if self._etcd_process is not None:
      process_status.append('etcd exit code: %s' % self._etcd_process.poll())
    return ', '.join(process_status) or 'no process status available'

  def __del__(self):
    # __del__ may run during interpreter shutdown when module globals can
    # already be torn down; swallow any cleanup failures so we don't print
    # a noisy traceback.
    try:
      if self._num_loaded_models_publisher is not None:
        self._num_loaded_models_publisher.stop()
    except Exception:  # pylint: disable=broad-except
      pass
    try:
      self._stop_processes()
    except Exception:  # pylint: disable=broad-except
      pass

  def _uses_embedded_etcd(self) -> bool:
    return (
        self._use_dynamo and
        _uses_etcd_discovery(self._dynamo_frontend_kwargs) and
        _uses_etcd_discovery(self._vllm_server_kwargs) and
        'ETCD_ENDPOINTS' not in os.environ)

  def _wait_for_etcd(self, endpoint: str, timeout_secs=30) -> None:
    deadline = time.time() + timeout_secs
    health_url = endpoint.rstrip('/') + '/health'
    while time.time() < deadline and self._etcd_process.poll() is None:
      try:
        with urllib.request.urlopen(health_url, timeout=2) as response:
          if response.status < 500:
            return
      except Exception:  # pylint: disable=broad-except
        time.sleep(1)

    process_status = self._process_status()
    self._stop_processes()
    raise RuntimeError(
        "Failed to start embedded etcd for Dynamo. Process status: "
        f"{process_status}. Install etcd in the worker container or set "
        "ETCD_ENDPOINTS to an external etcd service.")

  def _ensure_etcd(self) -> None:
    if not self._uses_embedded_etcd():
      return
    if shutil.which('etcd') is None:
      raise RuntimeError(
          "Embedded Dynamo mode requires etcd when ETCD_ENDPOINTS is not "
          "set. Install etcd in the worker container or set ETCD_ENDPOINTS "
          "to an external etcd service.")

    etcd_name = f'beam-dynamo-etcd-{uuid.uuid4().hex}'
    self._etcd_data_dir = f'/tmp/{etcd_name}'
    peer_port, = subprocess_server.pick_port(None)
    etcd_cmd = [
        'etcd',
        '--name',
        etcd_name,
        '--listen-client-urls',
        'http://127.0.0.1:{{PORT}}',
        '--advertise-client-urls',
        'http://127.0.0.1:{{PORT}}',
        '--listen-peer-urls',
        f'http://127.0.0.1:{peer_port}',
        '--initial-advertise-peer-urls',
        f'http://127.0.0.1:{peer_port}',
        '--initial-cluster',
        f'{etcd_name}=http://127.0.0.1:{peer_port}',
        '--data-dir',
        self._etcd_data_dir,
        '--log-level',
        'warn',
    ]
    self._etcd_process, etcd_port = start_process(etcd_cmd)
    endpoint = f'http://127.0.0.1:{etcd_port}'
    os.environ['ETCD_ENDPOINTS'] = endpoint
    self._managed_etcd_endpoint = endpoint
    self._wait_for_etcd(endpoint)

  def start_server(self, retries=3):
    with self._server_process_lock:
      if not self._server_started:
        self._stop_processes()
        self._ensure_etcd()
        if self._use_dynamo:
          # Dynamo embedded mode uses the frontend as its OpenAI-compatible
          # local endpoint and a separate vLLM worker process.
          server_cmd = [
              sys.executable,
              '-m',
              'dynamo.frontend',
              '--http-port',
              '{{PORT}}',
          ]
          _append_kwargs(server_cmd, self._dynamo_frontend_kwargs)
        else:
          server_cmd = [
              sys.executable,
              '-m',
              'vllm.entrypoints.openai.api_server',
              '--model',
              self._model_name,
              '--port',
              '{{PORT}}',
          ]
          _append_kwargs(server_cmd, self._vllm_server_kwargs)
        self._server_process, self._server_port = start_process(server_cmd)

        if self._use_dynamo:
          server_cmd = [
              sys.executable,
              '-m',
              'dynamo.vllm',
              '--model',
              self._model_name,
          ]
          _append_kwargs(server_cmd, self._vllm_server_kwargs)
          self._dynamo_process, _ = start_process(server_cmd)

      self.check_connectivity(retries)
      if self._num_loaded_models_publisher is not None:
        self._num_loaded_models_publisher.start()

  def get_server_port(self) -> int:
    if not self._server_started:
      self.start_server()
    return self._server_port

  def check_connectivity(self, retries=3, timeout_secs=600):
    start_time = time.time()
    with getVLLMClient(self._server_port) as client:
      while (time.time() - start_time < timeout_secs and
             self._server_process.poll() is None and
             (self._dynamo_process is None or
              self._dynamo_process.poll() is None) and
             (self._etcd_process is None or self._etcd_process.poll() is None)):
        try:
          models = client.models.list().data
          logging.info('models: %s' % models)
          if len(models) > 0:
            self._server_started = True
            return
        except:  # pylint: disable=bare-except
          pass
        # Sleep while bringing up the process
        time.sleep(5)

      process_status = self._process_status()
      self._stop_processes()
      if retries == 0:
        raise Exception(
            "Failed to start vLLM server. Process status: "
            f"{process_status}. Next time a request is tried, the server "
            "will be restarted")
      else:
        self.start_server(retries - 1)


class VLLMCompletionsModelHandler(ModelHandler[str,
                                               PredictionResult,
                                               _VLLMModelServer]):
  def __init__(
      self,
      model_name: str,
      vllm_server_kwargs: Optional[dict[str, Optional[str]]] = None,
      *,
      use_dynamo: bool = False,
      dynamo_frontend_kwargs: Optional[dict[str, Optional[str]]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      max_batch_weight: Optional[int] = None,
      element_size_fn: Optional[Callable[[Any], int]] = None,
      batch_length_fn: Optional[Callable[[Any], int]] = None,
      batch_bucket_boundaries: Optional[list[int]] = None,
      enable_autoscaling_metric: bool = True,
      autoscaling_tag: Optional[str] = None,
      autoscaling_poll_interval_secs:
      float = _VLLM_DEFAULT_AUTOSCALING_POLL_SECS,
      autoscaling_include_waiting: bool = False,
      autoscaling_aggregation: str = _VLLM_DEFAULT_AUTOSCALING_AGGREGATION,
      autoscaling_aggregation_window_secs: float = (
          _VLLM_DEFAULT_AUTOSCALING_WINDOW_SECS),
      load_signal_fn: Optional[Callable[[int], Optional[float]]] = None):
    """Implementation of the ModelHandler interface for vLLM using text as
    input.

    Example Usage::

      pcoll | RunInference(VLLMModelHandler(model_name='facebook/opt-125m'))

    Args:
      model_name: The vLLM model. See
        https://docs.vllm.ai/en/latest/models/supported_models.html for
        supported models.
      vllm_server_kwargs: Any additional kwargs to be passed into your vllm
        server when it is being created. When ``use_dynamo`` is disabled,
        this is invoked using ``python -m vllm.entrypoints.openai.api_server
        <beam provided args> <vllm_server_kwargs>``. When ``use_dynamo`` is
        enabled, these kwargs are passed to the ``dynamo.vllm`` worker
        process. For example, you could pass ``{'echo': 'true'}`` to prepend
        new messages with the previous message. On ~16GB GPUs, pass lower
        ``max-num-seqs`` and ``gpu-memory-utilization`` values (see
        ``apache_beam.examples.inference.vllm_text_completion``). For a list
        of possible kwargs, see
        https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#extra-parameters-for-completions-api
      use_dynamo: Whether to use NVIDIA Dynamo as the underlying vLLM engine.
        Requires installing Dynamo in your runtime environment
        (``pip install ai-dynamo[vllm]``). This is an opt-in single-worker
        embedded mode; KV-aware routing, disaggregated prefill/decode, KVBM
        offload across nodes, the Planner, and Grove are not active in
        embedded mode. Dynamo also requires an etcd-style discovery service:
        when ``ETCD_ENDPOINTS`` is unset, Beam starts a local etcd, which
        requires the ``etcd`` binary in the worker environment.
      dynamo_frontend_kwargs: Additional kwargs to be passed to the
        ``dynamo.frontend`` process when ``use_dynamo`` is enabled. By
        default, embedded Dynamo uses etcd discovery, TCP request plane, ZMQ
        event plane, round-robin routing, and disables router KV events.
      min_batch_size: optional. the minimum batch size to use when batching
        inputs.
      max_batch_size: optional. the maximum batch size to use when batching
        inputs.
      max_batch_duration_secs: optional. the maximum amount of time to buffer
        a batch before emitting; used in streaming contexts.
      max_batch_weight: optional. the maximum total weight of a batch.
      element_size_fn: optional. a function that returns the size (weight) of
        an element.
      batch_length_fn: optional. a callable that returns the length of an
        element for length-aware batching.
      batch_bucket_boundaries: optional. a sorted list of positive boundary
        values for length-aware batching buckets.
      enable_autoscaling_metric: if True (default), publish the
        ``num_loaded_models_<tag>`` autoscaling signal derived from the vLLM
        server's live request concurrency.
      autoscaling_tag: metric tag to publish under; defaults to a sanitized
        ``model_name``.
      autoscaling_poll_interval_secs: how often to sample and publish.
      autoscaling_include_waiting: if True, add ``vllm:num_requests_waiting``
        to the running count (default False: running only).
      autoscaling_aggregation: how the published num_loaded_models is reduced
        from observed concurrency -- ``'lifetime_max'`` (default; monotonic max
        over the whole run, never decays), ``'max'``/``'p90'``/``'p95'``/
        ``'mean'`` (over a trailing window), or ``'last'`` (raw). Avoids
        scaling down the instant an input burst clears.
      autoscaling_aggregation_window_secs: trailing window (seconds) for the
        windowed modes; ignored by ``'lifetime_max'`` (default 60).
      load_signal_fn: optional override that, given the server port, returns the
        raw load value directly, bypassing the default metric scrape.
    """
    super().__init__(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        max_batch_weight=max_batch_weight,
        element_size_fn=element_size_fn,
        batch_length_fn=batch_length_fn,
        batch_bucket_boundaries=batch_bucket_boundaries)
    self._model_name = model_name
    self._vllm_server_kwargs: dict[str, Optional[str]] = ({
        **_DYNAMO_ENGINE_DEFAULT_KWARGS, **(vllm_server_kwargs or {})
    } if use_dynamo else vllm_server_kwargs or {})
    self._dynamo_frontend_kwargs: dict[str, Optional[str]] = {
        **_DYNAMO_FRONTEND_DEFAULT_KWARGS, **(dynamo_frontend_kwargs or {})
    }
    self._use_dynamo = use_dynamo
    self._enable_autoscaling_metric = enable_autoscaling_metric
    self._autoscaling_tag = autoscaling_tag
    self._autoscaling_poll_interval_secs = autoscaling_poll_interval_secs
    self._autoscaling_include_waiting = autoscaling_include_waiting
    self._autoscaling_aggregation = autoscaling_aggregation
    self._autoscaling_aggregation_window_secs = (
        autoscaling_aggregation_window_secs)
    self._load_signal_fn = load_signal_fn

  def _resolved_autoscaling_tag(self) -> Optional[str]:
    if not self._enable_autoscaling_metric:
      return None
    return self._autoscaling_tag or _sanitize_metric_tag(self._model_name)

  def load_model(self) -> _VLLMModelServer:
    return _VLLMModelServer(
        self._model_name,
        self._vllm_server_kwargs,
        self._dynamo_frontend_kwargs,
        self._use_dynamo,
        autoscaling_tag=self._resolved_autoscaling_tag(),
        autoscaling_poll_interval_secs=self._autoscaling_poll_interval_secs,
        autoscaling_include_waiting=self._autoscaling_include_waiting,
        autoscaling_aggregation=self._autoscaling_aggregation,
        autoscaling_aggregation_window_secs=(
            self._autoscaling_aggregation_window_secs),
        load_signal_fn=self._load_signal_fn)

  async def _async_run_inference(
      self,
      batch: Sequence[str],
      model: _VLLMModelServer,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    inference_args = inference_args or {}

    async with getAsyncVLLMClient(model.get_server_port()) as client:
      try:
        async_predictions = [
            client.completions.create(
                model=self._model_name, prompt=prompt, **inference_args)
            for prompt in batch
        ]
        responses = await asyncio.gather(*async_predictions)
      except Exception as e:
        model.check_connectivity()
        raise e

    return [PredictionResult(x, y) for x, y in zip(batch, responses)]

  def run_inference(
      self,
      batch: Sequence[str],
      model: _VLLMModelServer,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of text strings.

    Args:
      batch: A sequence of examples as text strings.
      model: A _VLLMModelServer containing info for connecting to the server.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return asyncio.run(self._async_run_inference(batch, model, inference_args))

  def validate_inference_args(self, inference_args: Optional[dict[str, Any]]):
    # Override the base validator so OpenAI-compatible request kwargs such as
    # ``max_tokens`` can be passed through ``RunInference`` to the vLLM /
    # Dynamo server.
    pass

  def share_model_across_processes(self) -> bool:
    return True


class VLLMChatModelHandler(ModelHandler[Sequence[OpenAIChatMessage],
                                        PredictionResult,
                                        _VLLMModelServer]):
  def __init__(
      self,
      model_name: str,
      chat_template_path: Optional[str] = None,
      vllm_server_kwargs: Optional[dict[str, Optional[str]]] = None,
      *,
      use_dynamo: bool = False,
      dynamo_frontend_kwargs: Optional[dict[str, Optional[str]]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None,
      max_batch_weight: Optional[int] = None,
      element_size_fn: Optional[Callable[[Any], int]] = None,
      batch_length_fn: Optional[Callable[[Any], int]] = None,
      batch_bucket_boundaries: Optional[list[int]] = None,
      enable_autoscaling_metric: bool = True,
      autoscaling_tag: Optional[str] = None,
      autoscaling_poll_interval_secs:
      float = _VLLM_DEFAULT_AUTOSCALING_POLL_SECS,
      autoscaling_include_waiting: bool = False,
      autoscaling_aggregation: str = _VLLM_DEFAULT_AUTOSCALING_AGGREGATION,
      autoscaling_aggregation_window_secs: float = (
          _VLLM_DEFAULT_AUTOSCALING_WINDOW_SECS),
      load_signal_fn: Optional[Callable[[int], Optional[float]]] = None):
    """ Implementation of the ModelHandler interface for vLLM using previous
    messages as input.

    Example Usage::

      pcoll | RunInference(VLLMModelHandler(model_name='facebook/opt-125m'))

    Args:
      model_name: The vLLM model. See
        https://docs.vllm.ai/en/latest/models/supported_models.html for
        supported models.
      chat_template_path: Path to a chat template. This file must be accessible
        from your runner's execution environment, so it is recommended to use
        a cloud based file storage system (e.g. Google Cloud Storage).
        For info on chat templates, see:
        https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#chat-template
      vllm_server_kwargs: Any additional kwargs to be passed into your vllm
        server when it is being created. When ``use_dynamo`` is disabled,
        this is invoked using ``python -m vllm.entrypoints.openai.api_server
        <beam provided args> <vllm_server_kwargs>``. When ``use_dynamo`` is
        enabled, these kwargs are passed to the ``dynamo.vllm`` worker
        process. For example, you could pass ``{'echo': 'true'}`` to prepend
        new messages with the previous message. For a list of possible
        kwargs, see
        https://docs.vllm.ai/en/latest/serving/openai_compatible_server.html#extra-parameters-for-chat-api
      use_dynamo: Whether to use NVIDIA Dynamo as the underlying vLLM engine.
        Requires installing Dynamo in your runtime environment
        (``pip install ai-dynamo[vllm]``). This is an opt-in single-worker
        embedded mode; KV-aware routing, disaggregated prefill/decode, KVBM
        offload across nodes, the Planner, and Grove are not active in
        embedded mode. Dynamo also requires an etcd-style discovery service:
        when ``ETCD_ENDPOINTS`` is unset, Beam starts a local etcd, which
        requires the ``etcd`` binary in the worker environment.
      dynamo_frontend_kwargs: Additional kwargs to be passed to the
        ``dynamo.frontend`` process when ``use_dynamo`` is enabled. By
        default, embedded Dynamo uses etcd discovery, TCP request plane, ZMQ
        event plane, round-robin routing, and disables router KV events.
      min_batch_size: optional. the minimum batch size to use when batching
        inputs.
      max_batch_size: optional. the maximum batch size to use when batching
        inputs.
      max_batch_duration_secs: optional. the maximum amount of time to buffer
        a batch before emitting; used in streaming contexts.
      max_batch_weight: optional. the maximum total weight of a batch.
      element_size_fn: optional. a function that returns the size (weight) of
        an element.
      batch_length_fn: optional. a callable that returns the length of an
        element for length-aware batching.
      batch_bucket_boundaries: optional. a sorted list of positive boundary
        values for length-aware batching buckets.
      enable_autoscaling_metric: if True (default), publish the
        ``num_loaded_models_<tag>`` autoscaling signal derived from the vLLM
        server's live request concurrency.
      autoscaling_tag: metric tag to publish under; defaults to a sanitized
        ``model_name``.
      autoscaling_poll_interval_secs: how often to sample and publish.
      autoscaling_include_waiting: if True, add ``vllm:num_requests_waiting``
        to the running count (default False: running only).
      autoscaling_aggregation: how the published num_loaded_models is reduced
        from observed concurrency -- ``'lifetime_max'`` (default; monotonic max
        over the whole run, never decays), ``'max'``/``'p90'``/``'p95'``/
        ``'mean'`` (over a trailing window), or ``'last'`` (raw). Avoids
        scaling down the instant an input burst clears.
      autoscaling_aggregation_window_secs: trailing window (seconds) for the
        windowed modes; ignored by ``'lifetime_max'`` (default 60).
      load_signal_fn: optional override that, given the server port, returns the
        raw load value directly, bypassing the default metric scrape.
    """
    super().__init__(
        min_batch_size=min_batch_size,
        max_batch_size=max_batch_size,
        max_batch_duration_secs=max_batch_duration_secs,
        max_batch_weight=max_batch_weight,
        element_size_fn=element_size_fn,
        batch_length_fn=batch_length_fn,
        batch_bucket_boundaries=batch_bucket_boundaries)
    self._model_name = model_name
    self._vllm_server_kwargs: dict[str, Optional[str]] = ({
        **_DYNAMO_ENGINE_DEFAULT_KWARGS, **(vllm_server_kwargs or {})
    } if use_dynamo else vllm_server_kwargs or {})
    self._dynamo_frontend_kwargs: dict[str, Optional[str]] = {
        **_DYNAMO_FRONTEND_DEFAULT_KWARGS, **(dynamo_frontend_kwargs or {})
    }
    self._chat_template_path = chat_template_path
    self._chat_file = f'template-{uuid.uuid4().hex}.jinja'
    self._use_dynamo = use_dynamo
    self._enable_autoscaling_metric = enable_autoscaling_metric
    self._autoscaling_tag = autoscaling_tag
    self._autoscaling_poll_interval_secs = autoscaling_poll_interval_secs
    self._autoscaling_include_waiting = autoscaling_include_waiting
    self._autoscaling_aggregation = autoscaling_aggregation
    self._autoscaling_aggregation_window_secs = (
        autoscaling_aggregation_window_secs)
    self._load_signal_fn = load_signal_fn

  def _resolved_autoscaling_tag(self) -> Optional[str]:
    if not self._enable_autoscaling_metric:
      return None
    return self._autoscaling_tag or _sanitize_metric_tag(self._model_name)

  def load_model(self) -> _VLLMModelServer:
    chat_template_contents = ''
    if self._chat_template_path is not None:
      local_chat_template_path = os.path.join(os.getcwd(), self._chat_file)
      if not os.path.exists(local_chat_template_path):
        with FileSystems.open(self._chat_template_path) as fin:
          chat_template_contents = fin.read().decode()
        with open(local_chat_template_path, 'a') as f:
          f.write(chat_template_contents)
      self._vllm_server_kwargs['chat_template'] = local_chat_template_path

    return _VLLMModelServer(
        self._model_name,
        self._vllm_server_kwargs,
        self._dynamo_frontend_kwargs,
        self._use_dynamo,
        autoscaling_tag=self._resolved_autoscaling_tag(),
        autoscaling_poll_interval_secs=self._autoscaling_poll_interval_secs,
        autoscaling_include_waiting=self._autoscaling_include_waiting,
        autoscaling_aggregation=self._autoscaling_aggregation,
        autoscaling_aggregation_window_secs=(
            self._autoscaling_aggregation_window_secs),
        load_signal_fn=self._load_signal_fn)

  async def _async_run_inference(
      self,
      batch: Sequence[Sequence[OpenAIChatMessage]],
      model: _VLLMModelServer,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    inference_args = inference_args or {}

    async with getAsyncVLLMClient(model.get_server_port()) as client:
      try:
        async_predictions = [
            client.chat.completions.create(
                model=self._model_name,
                messages=[{
                    "role": message.role, "content": message.content
                } for message in messages],
                **inference_args) for messages in batch
        ]
        predictions = await asyncio.gather(*async_predictions)
      except Exception as e:
        model.check_connectivity()
        raise e

    return [PredictionResult(x, y) for x, y in zip(batch, predictions)]

  def run_inference(
      self,
      batch: Sequence[Sequence[OpenAIChatMessage]],
      model: _VLLMModelServer,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inferences on a batch of text strings.

    Args:
      batch: A sequence of examples as OpenAI messages.
      model: A _VLLMModelServer for connecting to the spun up server.
      inference_args: Any additional arguments for an inference.

    Returns:
      An Iterable of type PredictionResult.
    """
    return asyncio.run(self._async_run_inference(batch, model, inference_args))

  def validate_inference_args(self, inference_args: Optional[dict[str, Any]]):
    # Override the base validator so OpenAI-compatible request kwargs such as
    # ``max_tokens`` can be passed through ``RunInference`` to the vLLM /
    # Dynamo server.
    pass

  def share_model_across_processes(self) -> bool:
    return True
