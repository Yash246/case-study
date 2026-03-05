"""
Abstract base class for all data transformation modules (Silver / Gold layers) .
"""

import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

import polars as pl

logger = logging.getLogger(__name__)


class BaseTransformation(ABC):
    """
    Base class for every transformation module.

    Args:
        output_folder: Folder where the transformed parquet file is
            written. Created automatically if it does not exist.
    """

    def __init__(self, output_folder: Path):

        self._output_folder = output_folder
        self._output_folder.mkdir(parents=True, exist_ok=True)


    @property
    def output_folder(self) -> Path:
        """
        Return the output directory path for this transformation.
        """

        return self._output_folder

    def transform(self, data: Any, **kwargs: Any) -> pl.DataFrame:
        """
        Orchestrate the full transformation workflow.

        Steps executed in order:
            1. _pre_transform() — input preparation.
            2. _apply_transforms() — core transformation logic.
            3. _post_transform() — data enrichment.
            4. _write() — write output parquet.

        Args:
            data: Input data from the previous layer.
            **kwargs: Additional context passed through to each step.

        Returns:
            The fully transformed dataframe.
        """

        logger.info("Transformation starting")

        data = self._pre_transform(data, **kwargs)

        logger.debug("prep transformation completed")

        result = self._apply_transforms(data, **kwargs)

        result = self._post_transform(result, **kwargs)

        output_path = self._write(result)

        logger.info("transformation completed.")

        return result

    @abstractmethod
    def _apply_transforms(self, data: Any, **kwargs: Any) -> pl.DataFrame:
        """
        Apply core transformation logic.

        Args:
            data: Pre processed input data.
            **kwargs: Additional context.

        Returns:
            Transformed dataframe.
        """

    @abstractmethod
    def _get_output_filename(self) -> str:
        """
        Return the output parquet filename.

        Returns:
            Filename string
        """

    def _pre_transform(self, data: Any, **kwargs: Any) -> Any:
        """
        Preparatory transformations.

        Args:
            data: Raw input data.
            **kwargs: Additional context.

        Returns:
            Optionally modified input data.
        """

        return data

    def _post_transform(self, df: pl.DataFrame, **kwargs: Any) -> pl.DataFrame:
        """
        Post processing transformations.

        Args:
            df: Transformed DataFrame.
            **kwargs: Additional context.

        Returns:
            Optionally enriched DataFrame.
        """

        return df

    def _write(self, df: pl.DataFrame) -> Path:
        """
        Write the transformed dataframe to parquet.

        Args:
            df: dataframe to write.

        Returns:
            path to the written parquet file.
        """

        output_path = self._output_folder / self._get_output_filename()

        df.write_parquet(output_path)

        return output_path
