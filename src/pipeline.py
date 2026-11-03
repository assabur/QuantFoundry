import json
import logging
import time
from uuid import uuid4

from src.engines import create_engine

LOGGER = logging.getLogger('quantfoundry.ingestion')


class Pipeline:
    """Run one job with one compute engine and emit measured stage metrics."""

    def __init__(self, config, spark=None, engine=None):
        self.config = config
        self.spark = spark
        self.engine = engine
        self.metrics = {}
        self._counts = {}

    def _measure(self, stage, name, action, dataframe=False, rows=None):
        start = time.perf_counter()
        record = {'stage': stage, 'name': name, 'status': 'success'}
        try:
            result = action()
            if dataframe:
                rows = self.engine.row_count(result)
                self._counts[id(result)] = rows
            record['rows'] = rows
            return result
        except Exception as exc:
            record.update(status='failed', error_type=type(exc).__name__)
            raise
        finally:
            elapsed = time.perf_counter() - start
            record['duration_s'] = elapsed
            if rows is not None:
                record['rows_per_second'] = rows / elapsed if elapsed else None
            self.metrics['stages'].append(record)

    def set_extract_results(self, outputs):
        for config in self.config.inputs or []:
            outputs[config.name] = self._measure(
                'extract', config.name,
                lambda c=config: self.engine.extract(c), dataframe=True,
            )

    def set_quality_gate(self, outputs):
        for config in self.config.quality_gate or []:
            data = (
                {k: outputs[v] for k, v in config.input.items()}
                if isinstance(config.input, dict) else outputs[config.input]
            )
            self._measure(
                'quality', config.name,
                lambda c=config, d=data: self.engine.validate(c, d),
            )

    def set_transform_results(self, outputs):
        for config in self.config.transforms or []:
            data = None
            if isinstance(config.inputs, dict):
                data = {k: outputs[v] for k, v in config.inputs.items()}
            elif isinstance(config.inputs, str):
                data = outputs[config.inputs]
            outputs[config.name] = self._measure(
                'transform', config.name,
                lambda c=config, d=data: self.engine.transform(c, d),
                dataframe=True,
            )

    def set_load_results(self, outputs):
        for config in self.config.outputs or []:
            data = outputs[config.input]
            self._measure(
                'load', config.name,
                lambda c=config, d=data: self.engine.load(c, d),
                rows=self._counts[id(data)],
            )

    def run(self):
        start = time.perf_counter()
        cpu_start = time.process_time()
        self.metrics = {
            'event': 'ingestion_metrics', 'run_id': str(uuid4()),
            'job': self.config.name, 'engine': self.config.engine,
            'status': 'running', 'stages': [], 'engine_setup_s': 0.0,
        }
        self._counts = {}
        try:
            if self.engine is None:
                self.engine = create_engine(self.config.engine, self.spark)
            if self.engine.name != self.config.engine:
                raise ValueError('Configured engine and supplied engine differ')
            self.engine.check_config(self.config)
            setup_start = time.perf_counter()
            try:
                self.engine.start()
            finally:
                self.metrics['engine_setup_s'] = time.perf_counter() - setup_start
            self.spark = self.engine.spark
            outputs = {}
            self.set_extract_results(outputs)
            self.set_quality_gate(outputs)
            self.set_transform_results(outputs)
            self.set_load_results(outputs)
            self.metrics['status'] = 'success'
            return outputs
        except Exception as exc:
            self.metrics.update(
                status='failed', error_type=type(exc).__name__, error=str(exc),
            )
            raise
        finally:
            if self.engine is not None:
                try:
                    self.engine.close()
                except Exception:
                    LOGGER.exception('Engine cleanup failed')
            duration = time.perf_counter() - start
            stages = self.metrics['stages']
            inputs = sum(s.get('rows') or 0 for s in stages
                         if s['stage'] == 'extract')
            outputs = sum(s.get('rows') or 0 for s in stages
                          if s['stage'] == 'load' and s['status'] == 'success')
            self.metrics.update(
                duration_s=duration,
                python_cpu_s=time.process_time() - cpu_start,
                input_rows=inputs, output_rows=outputs,
                input_rows_per_second=inputs / duration if duration else None,
            )
            LOGGER.info(json.dumps(self.metrics, ensure_ascii=False))
