import logging

from src.pipeline import Pipeline
from src.engines.backend import SparkEngine

LOGGER = logging.getLogger('quantfoundry.wrapper')


class WrapperPipeline:
    """Execute ordered jobs, sharing a lazily initialized Spark session."""

    def __init__(self, configs, spark=None):
        self.configs = configs
        self.spark = spark
        self.status = []
        self.results = []
        self.errors = []

    def launch(self):
        engines = {}
        jobs = self.enable_jobs
        # Packages must be configured before the first shared Spark session.
        iceberg_required = any(
            config.engine == 'spark' and SparkEngine.requires_iceberg(config)
            for config in jobs
        )
        try:
            for config in jobs:
                pipeline = Pipeline(config, spark=self.spark)
                try:
                    pipeline.engine = engines.get(config.engine)
                    if config.engine == 'spark' and pipeline.engine is None:
                        pipeline.engine = SparkEngine(
                            spark=self.spark, iceberg_required=iceberg_required,
                        )
                    pipeline.run()
                    engines[config.engine] = pipeline.engine
                    status = 'Success'
                except Exception as exc:
                    status = 'Fails'
                    self.errors.append((config.name, exc))
                    LOGGER.exception('Job %s failed (%s)', config.name, config.engine)
                if pipeline.engine is not None:
                    engines[config.engine] = pipeline.engine
                self.results.append(pipeline.metrics)
                self.status.append((
                    config.name, pipeline.metrics.get('duration_s', 0), status,
                ))
        finally:
            backend = engines.get('spark')
            if backend and self.spark is None and backend.spark is not None:
                backend.spark.stop()
        return self.results

    @property
    def enable_jobs(self):
        return sorted(
            [config for config in self.configs.configs if config.enable],
            key=lambda config: config.order,
        )
