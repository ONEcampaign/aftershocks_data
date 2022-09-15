from dataclasses import dataclass


@dataclass(frozen=True, init=False)
class MapDataSchema:
    GEOMETRY: str = "geometry"
    FORMAL_NAME: str = "formal_name"
    NAME: str = "name"
    ISO_CODE: str = "iso_code"


@dataclass(frozen=True, init=False)
class BubbleDataSchema:
    FORMAL_NAME: str = "formal_name"
    NAME: str = "name"
    ISO_CODE: str = "iso_code"
    POSITION: str = "equal_position"
