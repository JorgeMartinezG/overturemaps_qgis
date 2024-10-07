import s3fs
import os
from tempfile import TemporaryDirectory
from argparse import ArgumentParser
from osgeo import ogr
from pathlib import Path
from utils import AWS_BUCKET_NAME, AWS_REGION, VERSION, get_boundaries

THEME_MAPPINGS = {"building": "buildings", "segment": "transportation"}


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

        if counter == 1000:
            break

    # Save and close DataSources

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

    # Create bucket if not exists.
    s3 = s3fs.S3FileSystem(
        anon=False,
        client_kwargs={"region_name": AWS_REGION},
    )
    try:
        bucket_files = [Path(f).name for f in s3.ls(AWS_BUCKET_NAME)]
    except:
        s3.makedir(AWS_BUCKET_NAME)
        bucket_files = []

    pq_type, pq_theme = args.type
    input_path = f"{args.path}/theme={pq_theme}/type={pq_type}"

    item_ids = args.ids if args.ids is not None else []

    boundaries = get_boundaries(item_ids, with_geom=True)
    version = VERSION.replace("-", "").replace(".", "")

    for boundary in boundaries:
        print(f"Processing boundary for {boundary.name}")
        with TemporaryDirectory() as tmp_dir:
            file_name = f"{boundary.iso3}_{boundary.id}_{pq_theme}_{pq_type}_{version}.fgb"
            # Bucket already exists. Just skip it.
            if file_name in bucket_files:
                print(f"File {file_name} already created")
                continue

            output_path = os.path.join(tmp_dir, file_name)

            if boundary.wkb is None:
                continue
            create_file(input_path, output_path, boundary.wkb, pq_type)

            print(f"Uploading file to s3: {file_name}")
            s3.put_file(output_path, os.path.join(AWS_BUCKET_NAME, file_name))


if __name__ == "__main__":
    main()
