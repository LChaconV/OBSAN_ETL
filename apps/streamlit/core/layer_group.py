
from dataclasses import dataclass, field
from typing import Union
from core.layer import GeoLayer


@dataclass
class LayerGroup:
    """
    Agrupa capas y subgrupos bajo una categoría en el panel lateral.

    Parámetros:
        id      — Identificador único del grupo
        label   — Nombre visible en el panel
        icon    — Emoji o texto corto que aparece junto al nombre
        items   — Lista de GeoLayer o LayerGroup anidados
        expanded — Si el grupo inicia expandido en el panel
    """
    id: str
    label: str
    icon: str = "📁"
    items: list = field(default_factory=list)   # GeoLayer | LayerGroup
    expanded: bool = True

    def add(self, item: Union[GeoLayer, "LayerGroup"]) -> "LayerGroup":
        """
        Agrega una capa o subgrupo al grupo.
        Retorna self para permitir encadenamiento:
            grupo.add(capa1).add(capa2)
        """
        self.items.append(item)
        return self

    def all_layers(self) -> list[GeoLayer]:
        """
        Retorna todas las GeoLayer dentro de este grupo (y subgrupos),
        de forma recursiva.
        """
        result = []
        for item in self.items:
            if isinstance(item, GeoLayer):
                result.append(item)
            elif isinstance(item, LayerGroup):
                result.extend(item.all_layers())
        return result

    def find_layer(self, layer_id: str) -> GeoLayer | None:
        """
        Busca una capa por su ID en todo el árbol.
        Retorna None si no la encuentra.
        """
        for layer in self.all_layers():
            if layer.id == layer_id:
                return layer
        return None

    def __repr__(self):
        n_layers = len(self.all_layers())
        return f"LayerGroup(id='{self.id}', label='{self.label}', capas={n_layers})"
