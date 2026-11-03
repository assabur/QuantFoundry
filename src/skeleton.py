from dataclasses import dataclass
from typing import Optional, Union, List, Dict, Any

from dataclasses_json import dataclass_json

"""
define the Skeleton of input file
"""


@dataclass_json
@dataclass
class InputConfig:
    name: str
    uri: Optional[str] = None
    extractor: str = "src.extract.Extractor"
    parameters: Optional[Dict[str, Any]] = None

    """
    redefine the beahaviour of equality of two instances
    """

    def __eq__(self, other):
        if not isinstance(other, InputConfig):
            return False
        return self.name == other.name and self.uri == other.uri and self.parameters == other.parameters


@dataclass
@dataclass_json
class ValidationConfig:
    def __init__(
        self,
        name: str,
        validator: str,
        input: str,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        self.prefix = "src.validators."
        self.name = name
        self.validator = validator if self.prefix in validator else self.prefix + validator
        self.input = input
        self.parameters = parameters
        print(self.validator)

    name: str
    validator: str
    input: str
    parameters: Optional[Dict[str, Any]] = None

    def __eq__(self, other):
        if isinstance(other, ValidationConfig):
            return self.name == other.name and self.validator == other.validator and self.input == other.input
        return False


"""
define the Skeleton of output data
"""


@dataclass_json
@dataclass
class OutputConfig:
    name: str
    uri: str
    input: Optional[str] = None
    loader: str = "src.load.Loader."
    parameters: Optional[Dict[str, Any]] = None

    """
     redefine the beahaviour of equality of two instances
     """

    def __eq__(self, other):
        if isinstance(other, OutputConfig):
            return (
                self.name == other.name
                and self.loader == other.loader
                and self.input == other.input
                and self.uri == other.uri
            )
        return False


@dataclass_json
@dataclass
class TransformConfig:
    def __init__(
        self,
        name,
        transformer: str,
        inputs: Union[Optional[Dict[str, str]], str] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ):
        self.prefix = "src.transform."
        self.name = name
        self.transformer = transformer if self.prefix in transformer else self.prefix + transformer
        self.inputs = inputs
        self.parameters = parameters

    name: str
    transformer: str
    inputs: Union[Optional[Dict[str, str]], str] = None
    parameters: Optional[Dict[str, Any]] = None

    def __eq__(self, other):
        if isinstance(other, TransformConfig):
            return self.name == other.name and self.transformer == other.transformer and self.inputs == other.inputs
        return False


@dataclass_json
@dataclass
class Config:
    name: str = "default_name"
    enable: bool = True
    order: int = 9999
    engine: str = "spark"
    quality_gate: Optional[List[ValidationConfig]] = None
    inputs: Optional[List[InputConfig]] = None
    transforms: Optional[List[TransformConfig]] = None
    outputs: Optional[List[OutputConfig]] = None

    def __eq__(self, other):
        if isinstance(other, Config):
            return (
                self.engine == other.engine
                and self.inputs == other.inputs
                and self.transforms == other.transforms
                and self.quality_gate == other.quality_gate
                and self.outputs == other.outputs
            )


@dataclass_json
@dataclass
class GroupConfig:
    configs: Optional[List[Config]] = None

    def __eq__(self, other):
        if isinstance(other, GroupConfig):
            return self.configs == other.configs
