import json
import yaml
import os
from dotenv import load_dotenv
from string import Template
from typing import List
from src.skeleton import Config, GroupConfig
from src.utils.config_handler import ConfigHandler
from src.utils.spark import SparkHelper


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class ExecutionContext(metaclass=Singleton):
    def __init__(self, specification_file: str):
        """
        :param :
        """

        self.specification_file = specification_file
        self.config_handler = ConfigHandler()
        self.spark = None

    def get_config(self, content_config) -> Config:
        render_text = self.render(content_config)

        render_text = yaml.load(render_text, Loader=yaml.FullLoader)
        render_text = json.dumps(render_text)
        return Config.from_json(render_text)

    def get_spark(self):
        spark_helper = SparkHelper()
        return spark_helper.get_spark()

    def get_configs(self) -> GroupConfig:
        configs = []
        for config in self.get_config_text():
            configs.append(self.get_config(config))

        return GroupConfig(configs=configs)

    # subtitutes Jinja values from yaml
    def render(self, content) -> str:
        load_dotenv()
        raw = os.getenv("RAW")
        silver = os.getenv("SILVER")
        gold = os.getenv("GOLD")
        template_config = Template(content)
        full_text = template_config.substitute(
            {
                "raw": raw,
                "silver": silver,
                "gold": gold,
            }
        )
        # print(full_text)

        return full_text

    def get_config_text(self) -> List[str]:
        return self.config_handler.read_as_string_local(path=self.specification_file)
