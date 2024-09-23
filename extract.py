import s3fs
import os
from tempfile import TemporaryDirectory
from argparse import ArgumentParser
from osgeo import ogr
from dataclasses import dataclass
from typing import List

BUCKET_NAME = "overturemaps-dumps"

THEME_MAPPINGS = {"building": "buildings", "segment": "transportation"}


@dataclass
class Boundary:
    iso3: str
    name: str
    rb: str
    wkb: bytes


def get_boundaries(maybe_iso3: List[str]) -> List[Boundary]:
    # Read shapefile layer.
    boundaries_drv = ogr.GetDriverByName("flatgeobuf")
    boundaries_ds = boundaries_drv.Open("./boundaries.fgb", 0)
    if boundaries_ds is None:
        raise ValueError("Boundaries file missing")

    boundaries_lyr = boundaries_ds.GetLayer()

    filter_str = ", ".join([f"'{i}'" for i in maybe_iso3])
    filter_str = f"iso3 IN ({filter_str})"

    boundaries_lyr.SetAttributeFilter(filter_str)

    boundaries = []
    for feature in boundaries_lyr:
        geom = feature.geometry()
        boundary = Boundary(
            iso3=feature["iso3"],
            name=feature["adm0_name"],
            rb=feature["rb"],
            wkb=geom.ExportToIsoWkb(),
        )
        boundaries.append(boundary)

    if len(boundaries) != len(maybe_iso3):
        iso3_boundaries = [i.iso3 for i in boundaries]
        missing = [i for i in maybe_iso3 if i not in iso3_boundaries]

        raise ValueError(f"iso3 codes not found {missing}")

    boundaries_ds = None

    return boundaries


def get_theme(type: str):
    if type not in THEME_MAPPINGS.keys():
        raise ValueError("theme not found")

    return (type, THEME_MAPPINGS[type])


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

    # Save and close DataSources

    input_ds = None
    output_ds = None


def main():
    parser = ArgumentParser()

    parser.add_argument(
        "--iso3",
        dest="iso3",
        help="Country iso3 codes (comma separated)",
        type=lambda x: [i for i in x.split(",")],
        required=True,
    )
    parser.add_argument(
        "--type",
        dest="type",
        type=lambda x: get_theme(x),
        help="Overture maps type",
        required=True,
    )
    parser.add_argument(
        "--path",
        dest="path",
        help="Dataset path",
        required=True,
    )
    args = parser.parse_args()

    s3 = s3fs.S3FileSystem(
        anon=False,
        client_kwargs={"region_name": os.getenv("AWS_DEFAULT_REGION")},
    )

    # Create bucket if not exists.
    try:
        s3.ls(BUCKET_NAME)
    except:
        s3.makedir(BUCKET_NAME)
    pq_type, pq_theme = args.type
    input_path = f"{args.path}/theme={pq_theme}/type={pq_type}"
    boundaries = get_boundaries(args.iso3)

    for boundary in boundaries:
        print(f"Processing boundary for {boundary.name}")
        with TemporaryDirectory() as tmp_dir:
            file_name = f"{boundary.iso3.lower()}_{pq_theme}_{pq_type}.fgb"
            output_path = os.path.join(tmp_dir, file_name)

            create_file(input_path, output_path, boundary.wkb, pq_type)

            print(f"Uploading file to s3: {file_name}")
            s3.put_file(output_path, os.path.join(BUCKET_NAME, file_name))


if __name__ == "__main__":
    main()
