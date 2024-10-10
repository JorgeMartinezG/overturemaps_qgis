import s3fs
import os
import shutil
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
from tempfile import TemporaryDirectory
from argparse import ArgumentParser
from dataclasses import dataclass
from osgeo import ogr, osr
from pathlib import Path
from typing import Tuple, List, Any, Optional
from utils import (
    AWS_BUCKET_NAME,
    AWS_REGION,
    VERSION,
    get_boundaries,
    Extent,
    Boundary,
)

Schema = List[Tuple[str, int]]


@dataclass
class Theme:
    om_theme: str
    om_type: str
    schema: Schema
    geom_type: int


BUILDING_SCHEMA = [
    ("id", ogr.OFTString),
    ("subtype", ogr.OFTString),
    ("class", ogr.OFTString),
    ("level", ogr.OFTInteger),
    ("height", ogr.OFTReal),
]


SEGMENT_SCHEMA = [
    ("id", ogr.OFTString),
    ("subtype", ogr.OFTString),
    ("class", ogr.OFTString),
    ("subclass", ogr.OFTString),
]


THEME_MAPPINGS = [
    Theme(
        om_theme="buildings",
        om_type="building",
        schema=BUILDING_SCHEMA,
        geom_type=ogr.wkbMultiPolygon,
    ),
    Theme(
        om_theme="transportation",
        om_type="segment",
        schema=SEGMENT_SCHEMA,
        geom_type=ogr.wkbLineString,
    ),
]


def get_theme(type: str) -> Theme:
    theme = next((i for i in THEME_MAPPINGS if i.om_type == type), None)

    if theme is None:
        raise ValueError("theme not found")
    return theme


def geoarrow_schema_adapter(schema: pa.Schema) -> pa.Schema:
    geometry_field_index = schema.get_field_index("geometry")
    geometry_field = schema.field(geometry_field_index)
    geoarrow_geometry_field = geometry_field.with_metadata(
        {b"ARROW:extension:name": b"geoarrow.wkb"}
    )

    geoarrow_schema = schema.set(geometry_field_index, geoarrow_geometry_field)

    return geoarrow_schema


def check_geometry(geom: ogr.Geometry) -> ogr.Geometry:
    geom_type = geom.GetGeometryType()

    if geom_type in (ogr.wkbMultiPolygon, ogr.wkbLineString):
        return geom

    if geom_type != ogr.wkbPolygon:
        raise ValueError("Invalid geometry type found", geom.GetGeometryName())

    multipolygon = ogr.Geometry(ogr.wkbMultiPolygon)
    multipolygon.AddGeometry(geom)

    return multipolygon


def row_to_feature(
    row: Any, layer_defn: ogr.FeatureDefn, boundary_geom: ogr.Geometry
) -> Optional[ogr.Feature]:
    geom = check_geometry(ogr.CreateGeometryFromWkb(row["geometry"]))
    # if geom.Intersects(boundary_geom) is False:
    #     return None

    feature = ogr.Feature(layer_defn)
    for i in range(layer_defn.GetFieldCount()):
        field_name = layer_defn.GetFieldDefn(i).GetNameRef()
        feature.SetField(field_name, row[field_name])

    feature.SetGeometry(geom)

    return feature


def get_data_from_bbox(
    s3_path: str,
    output_path: str,
    boundary: Boundary,
    layer_name: str,
    theme: Theme,
):
    xmin, xmax, ymin, ymax = boundary.extent

    filter = (
        (pc.field("bbox", "xmin") < xmax)
        & (pc.field("bbox", "xmax") > xmin)
        & (pc.field("bbox", "ymin") < ymax)
        & (pc.field("bbox", "ymax") > ymin)
    )

    dataset = ds.dataset(
        s3_path, filesystem=fs.S3FileSystem(anonymous=True, region="us-west-2")
    )

    batches = dataset.to_batches(filter=filter)
    non_empty_batches = (b for b in batches if b.num_rows > 0)

    geoarrow_schema = geoarrow_schema_adapter(dataset.schema)
    reader = pa.RecordBatchReader.from_batches(
        geoarrow_schema, non_empty_batches
    )

    # Create output layer.

    output_driver = ogr.GetDriverByName("flatgeobuf")
    output_ds = output_driver.CreateDataSource(output_path)

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)

    output_layer = output_ds.CreateLayer(
        layer_name,
        geom_type=theme.geom_type,
        srs=srs,
        options=["SPATIAL_INDEX=YES"],
    )

    for field_key, field_type in theme.schema:
        ogr_field = ogr.FieldDefn(field_key, field_type)
        output_layer.CreateField(ogr_field)

    layer_defn = output_layer.GetLayerDefn()

    counter = 1

    boundary_geom = ogr.CreateGeometryFromWkb(boundary.wkb)

    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if batch.num_rows == 0:
            continue

        features = [
            row_to_feature(row, layer_defn, boundary_geom)
            for row in batch.to_pylist()
        ]
        for feature in features:
            if feature is None:
                continue

            output_layer.CreateFeature(feature)
            feature = None

        counter += len(features)
        print(f"Processed {counter} features", end="\r")

    output_ds = None


def create_file(
    input_path: str, output_path: str, geom_wkb: bytes, layer_name: str
):
    geom_filter = ogr.CreateGeometryFromWkb(geom_wkb)

    input_driver = ogr.GetDriverByName("parquet")
    input_ds = input_driver.Open(input_path, 0)
    input_layer = input_ds.GetLayer()

    # Get input dataset schema
    defn = input_layer.GetLayerDefn()

    output_driver = ogr.GetDriverByName("flatgeobuf")
    output_ds = output_driver.CreateDataSource(output_path)

    output_layer = output_ds.CreateLayer(
        layer_name,
        geom_type=input_layer.GetGeomType(),
        srs=input_layer.GetSpatialRef(),
        options=["SPATIAL_INDEX=YES"],
    )

    for i in range(0, defn.GetFieldCount()):
        field_defn = defn.GetFieldDefn(i)
        if field_defn.GetType() == ogr.OFTStringList:
            field_defn = ogr.FieldDefn(field_defn.GetNameRef(), ogr.OFTString)
        output_layer.CreateField(field_defn)

    print("Processing features")
    # for count in range(0, buildings_layer.GetFeatureCount()):
    counter = 1
    input_layer.SetSpatialFilter(geom_filter)
    output_layer_defn = output_layer.GetLayerDefn()

    for feature in input_layer:
        output_feature = ogr.Feature(output_layer_defn)

        for i in range(0, output_layer_defn.GetFieldCount()):
            field_defn = output_layer_defn.GetFieldDefn(i)
            field_type = defn.GetFieldDefn(i).GetType()
            field_key = field_defn.GetNameRef()
            value = feature.GetField(i)

            if field_type == ogr.OFTStringList:
                value = ",".join(value)

            output_feature.SetField(field_key, value)
        output_feature.SetGeometry(feature.GetGeometryRef())
        if counter % 1e5 == 0:
            print(f"Processed {counter} features", end="\r")

        output_layer.CreateFeature(output_feature)

        output_feature = None
        counter += 1

        if counter == 1000:
            break

    # Save and close DataSources

    input_ds = None
    output_ds = None


def filter_by_boundary(
    input_path: str, output_path: str, geom_wkb: bytes
) -> None:
    geom_filter = ogr.CreateGeometryFromWkb(geom_wkb)

    input_driver = ogr.GetDriverByName("flatgeobuf")
    input_ds = input_driver.Open(input_path, 0)
    input_layer = input_ds.GetLayer()

    # Get input dataset schema
    defn = input_layer.GetLayerDefn()

    output_driver = ogr.GetDriverByName("flatgeobuf")
    output_ds = output_driver.CreateDataSource(output_path)

    output_layer = output_ds.CreateLayer(
        input_layer.GetName(),
        geom_type=input_layer.GetGeomType(),
        srs=input_layer.GetSpatialRef(),
        options=["SPATIAL_INDEX=YES"],
    )

    for i in range(0, defn.GetFieldCount()):
        field_defn = defn.GetFieldDefn(i)
        output_layer.CreateField(field_defn)

    print("Running exact filter")
    # for count in range(0, buildings_layer.GetFeatureCount()):
    counter = 1
    input_layer.SetSpatialFilter(geom_filter)
    output_layer_defn = output_layer.GetLayerDefn()

    for feature in input_layer:
        output_layer.CreateFeature(feature)

        output_feature = None
        counter += 1

    input_ds = None
    output_ds = None


def main():
    parser = ArgumentParser()

    parser.add_argument(
        "--ids",
        dest="ids",
        help="Object ids from geoenabler, comma separated",
        type=lambda x: [int(i) for i in x.split(",")],
    )
    parser.add_argument(
        "--type",
        dest="type_str",
        help="Overture maps type",
        required=True,
    )
    args = parser.parse_args()

    # Create bucket if not exists.
    s3 = s3fs.S3FileSystem(
        anon=False,
        client_kwargs={"region_name": AWS_REGION},
    )
    # try:
    #     bucket_files = [Path(f).name for f in s3.ls(AWS_BUCKET_NAME)]
    # except:
    #     s3.makedir(AWS_BUCKET_NAME)
    #     bucket_files = []

    theme = get_theme(args.type_str)

    remote_s3_path = f"overturemaps-us-west-2/release/{VERSION}/theme={theme.om_theme}/type={theme.om_type}/"

    item_ids = args.ids if args.ids is not None else []

    boundaries = get_boundaries(item_ids, with_geom=True)
    version = VERSION.replace("-", "").replace(".", "")

    for boundary in boundaries:
        print(f"Processing boundary for {boundary.name}")
        with TemporaryDirectory() as tmp_dir:
            file_name = f"{boundary.iso3}_{boundary.id}_{theme.om_theme}_{theme.om_type}_{version}.fgb"
            # Bucket already exists. Just skip it.
            # if file_name in bucket_files:
            #     print(f"File {file_name} already created")
            #     continue
            temp_path = os.path.join(tmp_dir, f"temp_{file_name}")
            output_path = os.path.join(tmp_dir, file_name)

            if boundary.wkb is None:
                continue

            get_data_from_bbox(
                remote_s3_path,
                temp_path,
                boundary,
                file_name,
                theme,
            )

            filter_by_boundary(temp_path, output_path, boundary.wkb)

            shutil.move(output_path, ".")

            # print(f"Uploading file to s3: {file_name}")
            # s3.put_file(output_path, os.path.join(AWS_BUCKET_NAME, file_name))


if __name__ == "__main__":
    main()
