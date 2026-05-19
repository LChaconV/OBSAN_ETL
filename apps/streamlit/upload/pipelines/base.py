"""
upload/pipelines/base.py — Clase base abstracta para pipelines ETL

Todos los pipelines deben heredar de BasePipeline e implementar
el método run(). El método log() está disponible para registrar
mensajes durante la ejecución.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class PipelineResult:
    success:  bool
    message:  str
    logs:     list[str] = field(default_factory=list)
    rows_processed: int = 0


class BasePipeline(ABC):
    """
    Clase base para todos los pipelines ETL del observatorio.
    """
    def __init__(self):
        self._logs: list[str] = []

    def log(self, msg: str):
        self._logs.append(msg)

    @abstractmethod
    def run(self, file_path: str) -> PipelineResult:
        """
        Ejecuta el pipeline ETL sobre el archivo en file_path.
        Debe retornar un PipelineResult.
        """
        pass
