import os
from osgeo import ogr
from dataclasses import dataclass
from typing import List, Optional


AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME", "overturemaps-extracts")
AWS_REGION = os.getenv("AWS_DEFAULT_REGION")
VERSION = "2024-08-20.0"


@dataclass
class Boundary:
    id: int
    iso3: str
    name: str
    wkb: Optional[bytes]


def get_boundaries(maybe_ids: List[int], with_geom: bool) -> List[Boundary]:
    # Read shapefile layer.
    boundaries_drv = ogr.GetDriverByName("flatgeobuf")
    boundaries_ds = boundaries_drv.Open("./boundaries.fgb", 0)
    if boundaries_ds is None:
        raise ValueError("Boundaries file missing")

    boundaries_lyr = boundaries_ds.GetLayer()

    if len(maybe_ids) > 0:
        filter_str = ", ".join([f"'{i}'" for i in maybe_ids])
        filter_str = f"objectid IN ({filter_str})"

        boundaries_lyr.SetAttributeFilter(filter_str)

    boundaries = []
    for feature in boundaries_lyr:
        if feature["rb"] == "-":
            continue

        geom = feature.geometry()
        boundary = Boundary(
            id=feature["objectid"],
            iso3=feature["iso3"],
            name=feature["adm0_name"],
            wkb=geom.ExportToIsoWkb() if with_geom is True else None,
        )
        boundaries.append(boundary)

    boundaries = sorted(boundaries, key=lambda x: x.name)

    if len(maybe_ids) == 0:
        return boundaries

    # Check for repeated s3 and merge geometries.
    if len(boundaries) != len(maybe_ids):
        object_ids = set([i.id for i in boundaries])
        missing = [i for i in maybe_ids if i not in object_ids]
        raise ValueError(f"object_ids not found {missing}")

    return boundaries
