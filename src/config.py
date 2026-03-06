"""
Configuration loader for the data pipeline.
"""

import logging
import yaml
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


class PipelineConfig:
    """
    Class for pipeline configuration values.

    Loads a yaml file once at construction time and provides
    properties for each configuration section.

    Args:
        config_path: path to the configuration yaml.

    Raises:
        FileNotFoundError: If config_path does not exist.
    """

    def __init__(self, config_path: str = "config/config.yaml"):

        self._config_path = Path(config_path)
        self._config: dict[str, Any] = self._load()

    def _load(self) -> dict[str, Any]:
        """
        Read and parse the configuration yaml.

        Returns:
            Parsed configuration dictionary.

        Raises:
            FileNotFoundError: If the configuration yaml does not exist.
        """

        logger.debug(f"Loading configuration from {self._config_path}")

        if not self._config_path.exists():
            logger.error(f"Config file not found: {self._config_path}")
            raise FileNotFoundError(f"Config file not found: {self._config_path}")

        with open(self._config_path, "r") as conf:
            config = yaml.safe_load(conf)

        logger.info(f"Configuration loaded successfully from {self._config_path}")

        return config

    @property
    def paths(self) -> dict[str, str]:
        """
        Return the paths for different data layers.
        """

        return self._config["paths"]

    @property
    def csv_files(self) -> dict[str, Any]:
        """
        Return the csv files section.
        """

        return self._config["csv_files"]

    @property
    def json_files(self) -> list[str]:
        """
        Return the json files section.
        """

        return self._config["json_files"]

    @property
    def xlsx_files(self) -> list[dict]:
        """
        Return the excel files section.
        """

        return self._config["xlsx_files"]

    @property
    def quality_rules(self) -> dict[str, Any]:
        """
        Return the quality_rules section with rule sets per table.
        """

        return self._config["quality_rules"]

    def raw_path(self) -> Path:
        """
        Return the path to the raw data layer.
        """

        return Path(self.paths["raw_data"])

    def bronze_path(self) -> Path:
        """
        Return the path to the Bronze data folder.
        """

        return Path(self.paths["bronze"])

    def silver_path(self) -> Path:
        """
        Return the path to the Silver data folder.
        """

        return Path(self.paths["silver"])

    def gold_path(self) -> Path:
        """
        Return the path to the Gold data folder.
        """

        return Path(self.paths["gold"])

    def watermark_path(self) -> Path:
        """
        Return the path to the watermark storage folder.
        """

        return Path(self.paths["watermark"])
