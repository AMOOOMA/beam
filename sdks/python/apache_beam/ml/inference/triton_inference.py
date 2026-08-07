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

"""RunInference ModelHandler for NVIDIA Triton Inference Server.

This mirrors ``vllm_inference.py``: each worker launches its own
``tritonserver`` subprocess (shared across the worker's processes) and forwards
requests to it over Triton's client API. Its distinguishing feature is that it
publishes the same ``num_loaded_models_<tag>`` autoscaling signal used by the
BeamML ModelManager (see ``base._NumLoadedModelsPublisher``), computed from
Triton's own live load metrics (by default
``nv_inference_pending_request_count`` -- the number of requests
queued/executing in parallel). This lets Dataflow's horizontal autoscaler scale
on real serving load instead of the always-1 count of loaded Triton servers.

``tritonclient`` and the ``tritonserver`` binary are expected to be provided by
the worker's custom container (they are not build-time dependencies here), the
same way ``vllm`` is for the vLLM handlers.
"""

import logging
import subprocess
import threading
import time
import urllib.request
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Sequence
from typing import Any
from typing import Optional

from apache_beam.ml.inference.base import ModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.vllm_inference import NumLoadedModelsPublisher
from apache_beam.ml.inference.vllm_inference import parse_prometheus_metric
from apache_beam.utils import subprocess_server

try:
  import tritonclient.http as triton_http
  logging.info('tritonclient.http successfully imported.')
except ModuleNotFoundError:
  triton_http = None
  logging.warning(
      'tritonclient module was not found. This is ok as long as the specified '
      'runner has tritonclient installed in its container.')

__all__ = [
    'TritonModelHandler',
]

_TRITON_DEFAULT_AUTOSCALING_POLL_SECS = 5.0
# Triton's default load gauge: requests received but not yet completed (queued
# plus executing) -- the closest analog to "requests running in parallel".
_TRITON_DEFAULT_LOAD_METRIC = 'nv_inference_pending_request_count'


def _sanitize_metric_tag(name: str) -> str:
  """Turns a model name into a metric-tag-safe token for the autoscaler.

  Args:
    name: The model name (may contain ``/``, ``:`` etc.).

  Returns:
    ``name`` with characters outside ``[A-Za-z0-9_]`` replaced by ``_``.
  """
  return ''.join(c if (c.isalnum() or c == '_') else '_' for c in name)


def _start_process(cmd: list[str]) -> subprocess.Popen:
  """Starts a subprocess and streams its stdout/stderr to Beam logging.

  Args:
    cmd: The command and arguments to execute.

  Returns:
    The started ``subprocess.Popen``.
  """
  logging.info("Starting Triton with %s", str(cmd).replace("',", "'"))
  process = subprocess.Popen(
      cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

  def log_stdout():
    line = process.stdout.readline()
    while line:
      logging.info(line.decode(errors='backslashreplace').rstrip())
      line = process.stdout.readline()

  t = threading.Thread(target=log_stdout)
  t.daemon = True
  t.start()
  return process


def _scrape_triton_load_signal(
    metrics_port: int,
    metric_name: str = _TRITON_DEFAULT_LOAD_METRIC,
    model_name: Optional[str] = None,
    timeout_secs: float = 5.0) -> Optional[float]:
  """Reads Triton's live load from its local Prometheus metrics endpoint.

  Args:
    metrics_port: Local port of Triton's metrics endpoint.
    metric_name: The Triton metric family to sum (default
      ``nv_inference_pending_request_count``).
    model_name: If set, only sum samples whose ``model`` label matches, so the
      signal reflects a single model rather than every model the server hosts.
    timeout_secs: Per-scrape HTTP timeout in seconds.

  Returns:
    The summed metric value, or ``None`` if it could not be scraped (which the
    publisher treats as "skip this tick").
  """
  url = f'http://localhost:{metrics_port}/metrics'
  try:
    with urllib.request.urlopen(url, timeout=timeout_secs) as response:
      text = response.read().decode('utf-8', errors='replace')
  except Exception:  # pylint: disable=broad-except
    return None
  label_filter = {'model': model_name} if model_name else None
  return parse_prometheus_metric(text, metric_name, label_filter=label_filter)


class _TritonModelServer():
  """Launches a local ``tritonserver`` and publishes its autoscaling load.

  One server is launched per worker (against ``model_repository``) on
  dynamically chosen ports, mirroring ``vllm_inference._VLLMModelServer``.
  """
  def __init__(
      self,
      model_repository: str,
      tritonserver_binary: str = 'tritonserver',
      server_extra_args: Optional[Sequence[str]] = None,
      *,
      model_name: Optional[str] = None,
      autoscaling_tag: Optional[str] = None,
      autoscaling_poll_interval_secs: float = (
          _TRITON_DEFAULT_AUTOSCALING_POLL_SECS),
      autoscaling_load_metric: str = _TRITON_DEFAULT_LOAD_METRIC,
      autoscaling_per_model: bool = True,
      load_signal_fn: Optional[Callable[[int], Optional[float]]] = None):
    self._model_repository = model_repository
    self._tritonserver_binary = tritonserver_binary
    self._server_extra_args = list(server_extra_args or [])
    self._model_name = model_name
    self._autoscaling_load_metric = autoscaling_load_metric
    self._autoscaling_per_model = autoscaling_per_model
    self._load_signal_fn = load_signal_fn

    self._server_process: Optional[subprocess.Popen] = None
    self._http_port: int = -1
    self._grpc_port: int = -1
    self._metrics_port: int = -1
    self._server_started = False
    self._server_process_lock = threading.RLock()

    # Publishes num_loaded_models_<tag> from Triton's live load metrics.
    # Started once the server is confirmed up; None disables the metric.
    self._num_loaded_models_publisher: Optional[NumLoadedModelsPublisher] = (
        NumLoadedModelsPublisher(
            tag=autoscaling_tag,
            signal_fn=self._get_load_signal,
            poll_interval_secs=autoscaling_poll_interval_secs)
        if autoscaling_tag else None)

    self.start_server()

  def _get_load_signal(self) -> Optional[float]:
    """Returns the current load-derived num_loaded_models value, or None.

    Returns None while the server is down/restarting so the publisher skips the
    tick instead of reporting a misleading 0.
    """
    if not self._server_started or self._metrics_port < 0:
      return None
    if self._load_signal_fn is not None:
      return self._load_signal_fn(self._metrics_port)
    model = self._model_name if self._autoscaling_per_model else None
    return _scrape_triton_load_signal(
        self._metrics_port,
        metric_name=self._autoscaling_load_metric,
        model_name=model)

  def _stop_process(self) -> None:
    process = self._server_process
    if process is None or process.poll() is not None:
      return
    # A process may exit between poll() and terminate()/kill(); treat that as
    # already-stopped so we don't abort the broader cleanup.
    try:
      process.terminate()
      try:
        process.wait(timeout=10)
      except subprocess.TimeoutExpired:
        process.kill()
        process.wait()
    except OSError:
      pass
    self._server_process = None
    self._server_started = False

  def __del__(self):
    try:
      if self._num_loaded_models_publisher is not None:
        self._num_loaded_models_publisher.stop()
    except Exception:  # pylint: disable=broad-except
      pass
    try:
      self._stop_process()
    except Exception:  # pylint: disable=broad-except
      pass

  def start_server(self, retries: int = 3) -> None:
    with self._server_process_lock:
      if not self._server_started:
        self._stop_process()
        self._http_port, self._grpc_port, self._metrics_port = (
            subprocess_server.pick_port(None, None, None))
        cmd = [
            self._tritonserver_binary,
            '--model-repository',
            self._model_repository,
            '--http-port',
            str(self._http_port),
            '--grpc-port',
            str(self._grpc_port),
            '--metrics-port',
            str(self._metrics_port),
            '--allow-metrics',
            'true',
        ]
        cmd.extend(self._server_extra_args)
        self._server_process = _start_process(cmd)
      self.check_connectivity(retries)
      if self._num_loaded_models_publisher is not None:
        self._num_loaded_models_publisher.start()

  def get_client_url(self) -> str:
    if not self._server_started:
      self.start_server()
    return f'localhost:{self._http_port}'

  def get_client(self) -> Any:
    if triton_http is None:
      raise RuntimeError(
          'tritonclient is not installed in this environment; install it in '
          'the worker container to use TritonModelHandler.')
    return triton_http.InferenceServerClient(url=self.get_client_url())

  def check_connectivity(self, retries: int = 3, timeout_secs: int = 600):
    start_time = time.time()
    ready_url = f'http://localhost:{self._http_port}/v2/health/ready'
    while (time.time() - start_time < timeout_secs and
           self._server_process is not None and
           self._server_process.poll() is None):
      try:
        with urllib.request.urlopen(ready_url, timeout=5) as response:
          if response.status == 200:
            self._server_started = True
            logging.info('Triton server is ready at %s', ready_url)
            return
      except Exception:  # pylint: disable=broad-except
        pass
      # Sleep while bringing up the process.
      time.sleep(5)

    exit_code = (
        self._server_process.poll()
        if self._server_process is not None else None)
    self._stop_process()
    if retries == 0:
      raise RuntimeError(
          'Failed to start Triton server (last exit code: '
          f'{exit_code}). Next time a request is tried, the server will be '
          'restarted.')
    self.start_server(retries - 1)


class TritonModelHandler(ModelHandler[Any, PredictionResult,
                                      _TritonModelServer]):
  """ModelHandler that serves inference from a per-worker Triton server.

  Example Usage::

    def to_inputs(example):
      inp = tritonclient.http.InferInput('INPUT0', example.shape, 'FP32')
      inp.set_data_from_numpy(example)
      return [inp]

    handler = TritonModelHandler(
        model_name='my_model',
        model_repository='/models',
        input_fn=to_inputs,
        output_names=['OUTPUT0'])
    pcoll | RunInference(handler)
  """
  def __init__(
      self,
      model_name: str,
      model_repository: str,
      model_version: str = '',
      input_fn: Optional[Callable[[Any], Any]] = None,
      output_names: Optional[Sequence[str]] = None,
      tritonserver_binary: str = 'tritonserver',
      server_extra_args: Optional[Sequence[str]] = None,
      *,
      enable_autoscaling_metric: bool = True,
      autoscaling_tag: Optional[str] = None,
      autoscaling_poll_interval_secs: float = (
          _TRITON_DEFAULT_AUTOSCALING_POLL_SECS),
      autoscaling_load_metric: str = _TRITON_DEFAULT_LOAD_METRIC,
      autoscaling_per_model: bool = True,
      load_signal_fn: Optional[Callable[[int], Optional[float]]] = None,
      min_batch_size: Optional[int] = None,
      max_batch_size: Optional[int] = None,
      max_batch_duration_secs: Optional[int] = None):
    """Implementation of the ModelHandler interface for Triton (launch mode).

    Args:
      model_name: Name of the model as registered in the Triton model
        repository.
      model_repository: Path to the Triton model repository the launched
        ``tritonserver`` loads models from (must be accessible in the worker
        container).
      model_version: Optional model version; empty string means "latest".
      input_fn: Callable mapping one example to the ``tritonclient`` inputs
        (a list of ``InferInput``) for a single request. If omitted, each
        example is assumed to already be such a list.
      output_names: Names of the model outputs to request and return. If
        omitted, Triton returns the model's default outputs.
      tritonserver_binary: Path/name of the ``tritonserver`` binary to launch.
      server_extra_args: Extra CLI args appended to the ``tritonserver``
        command.
      enable_autoscaling_metric: If True (default), publish the
        ``num_loaded_models_<tag>`` autoscaling signal derived from Triton's
        load metrics.
      autoscaling_tag: Metric tag to publish under; defaults to a sanitized
        ``model_name``.
      autoscaling_poll_interval_secs: How often to scrape and publish.
      autoscaling_load_metric: Triton metric family used as the load signal.
      autoscaling_per_model: If True, only count samples for this
        ``model_name`` (via the ``model`` label).
      load_signal_fn: Optional override that, given the metrics port, returns
        the signal value directly, bypassing the default metric scrape.
      min_batch_size: optional. Minimum batch size for batching inputs.
      max_batch_size: optional. Maximum batch size for batching inputs.
      max_batch_duration_secs: optional. Max time to buffer a batch (streaming).
    """
    self._model_name = model_name
    self._model_repository = model_repository
    self._model_version = model_version
    self._input_fn = input_fn
    self._output_names = list(output_names) if output_names else None
    self._tritonserver_binary = tritonserver_binary
    self._server_extra_args = list(server_extra_args or [])

    self._enable_autoscaling_metric = enable_autoscaling_metric
    self._autoscaling_tag = autoscaling_tag
    self._autoscaling_poll_interval_secs = autoscaling_poll_interval_secs
    self._autoscaling_load_metric = autoscaling_load_metric
    self._autoscaling_per_model = autoscaling_per_model
    self._load_signal_fn = load_signal_fn

    self._batching_kwargs: dict[str, Any] = {}
    if min_batch_size is not None:
      self._batching_kwargs['min_batch_size'] = min_batch_size
    if max_batch_size is not None:
      self._batching_kwargs['max_batch_size'] = max_batch_size
    if max_batch_duration_secs is not None:
      self._batching_kwargs['max_batch_duration_secs'] = max_batch_duration_secs

  def _resolved_autoscaling_tag(self) -> Optional[str]:
    if not self._enable_autoscaling_metric:
      return None
    return self._autoscaling_tag or _sanitize_metric_tag(self._model_name)

  def load_model(self) -> _TritonModelServer:
    return _TritonModelServer(
        model_repository=self._model_repository,
        tritonserver_binary=self._tritonserver_binary,
        server_extra_args=self._server_extra_args,
        model_name=self._model_name,
        autoscaling_tag=self._resolved_autoscaling_tag(),
        autoscaling_poll_interval_secs=self._autoscaling_poll_interval_secs,
        autoscaling_load_metric=self._autoscaling_load_metric,
        autoscaling_per_model=self._autoscaling_per_model,
        load_signal_fn=self._load_signal_fn)

  def _to_inputs(self, example: Any) -> Any:
    if self._input_fn is not None:
      return self._input_fn(example)
    return example

  def _requested_outputs(self) -> Optional[list[Any]]:
    if not self._output_names or triton_http is None:
      return None
    return [
        triton_http.InferRequestedOutput(name) for name in self._output_names
    ]

  def run_inference(
      self,
      batch: Sequence[Any],
      model: _TritonModelServer,
      inference_args: Optional[dict[str, Any]] = None
  ) -> Iterable[PredictionResult]:
    """Runs inference for a batch by issuing one Triton request per element.

    Args:
      batch: A sequence of examples (converted to Triton inputs via
        ``input_fn``).
      model: The ``_TritonModelServer`` to send requests to.
      inference_args: Extra kwargs forwarded to ``client.infer``.

    Returns:
      An Iterable of ``PredictionResult``; each ``inference`` is a dict of
      ``{output_name: numpy_array}`` (or Triton's raw response if
      ``output_names`` was not given).
    """
    inference_args = inference_args or {}
    client = model.get_client()
    requested_outputs = self._requested_outputs()
    results = []
    try:
      for example in batch:
        inputs = self._to_inputs(example)
        response = client.infer(
            model_name=self._model_name,
            inputs=inputs,
            model_version=self._model_version,
            outputs=requested_outputs,
            **inference_args)
        if self._output_names:
          prediction = {
              name: response.as_numpy(name)
              for name in self._output_names
          }
        else:
          prediction = response.get_response()
        results.append(PredictionResult(example, prediction))
    finally:
      close = getattr(client, 'close', None)
      if callable(close):
        close()
    return results

  def batch_elements_kwargs(self) -> dict[str, Any]:
    return self._batching_kwargs

  def validate_inference_args(self, inference_args: Optional[dict[str, Any]]):
    # Triton request kwargs (e.g. request_id, headers) are passed through.
    pass

  def share_model_across_processes(self) -> bool:
    return True
