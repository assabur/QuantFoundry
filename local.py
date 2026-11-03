import argparse
import logging

from src.execution_context import ExecutionContext
from src.wrapper_pipeline import WrapperPipeline


def main():
    parser = argparse.ArgumentParser(description='Run configured ingestion jobs')
    parser.add_argument('--config', default='config/market/polars.yaml')
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format='%(message)s')
    context = ExecutionContext(specification_file=args.config)
    wrapper = WrapperPipeline(configs=context.get_configs())
    wrapper.launch()
    if wrapper.errors:
        raise SystemExit(1)


if __name__ == '__main__':
    main()
