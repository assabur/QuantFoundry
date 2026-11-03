from src.engines.operations import apply_operations


class DataFrameTransformer:
    """Portable operations; the engine dispatches their native implementation."""

    def __init__(self, operations):
        self.operations = operations

    def transform(self, inputs, spark=None):
        return apply_operations(inputs, self.operations, 'spark')
