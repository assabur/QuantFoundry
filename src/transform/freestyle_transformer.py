import importlib
from typing import Dict
from pyspark.sql import DataFrame as SDF
from src.transform import AbstractTransform


class FreeStyleTransformer(AbstractTransform):
    def __init__(self, function_name: str, function_parameters=None) -> None:
        self.function_name = function_name
        self.function_parameters = function_parameters

    def transform(self, inputs: Dict[str, SDF], spark) -> SDF:
        module, name = (self.function_name.rsplit(".", 1)
                        if "." in self.function_name
                        else ("src.transform", self.function_name))
        function_of_transformation = getattr(importlib.import_module(module), name)
        return function_of_transformation(inputs, self.function_parameters)
