import s3fs
import os
from tempfile import TemporaryDirectory
from argparse import ArgumentParser
from osgeo import ogr

BUCKET_NAME = "overturemaps-dumps"

THEME_MAPPINGS = {"building": "buildings", "segment": "transportation"}


def get_country_geometry(iso3: str):
    # Read shapefile layer.
    countries_drv = ogr.GetDriverByName("flatgeobuf")
    countries_ds = countries_drv.Open("./countries.fgb", 0)
    countries_lyr = countries_ds.GetLayer()

    countries_lyr.SetAttributeFilter(f"iso3 = '{iso3}'")
    feature = next((f for f in countries_lyr), None)
    if feature is None:
        raise ValueError(f"Invalid iso3 code {iso3}")

    geom = feature.geometry()

    countries_ds = None

    return geom.ExportToIsoWkb()


def get_theme(type: str):
    if type not in THEME_MAPPINGS.keys():
        raise ValueError("theme not found")

    return (type, THEME_MAPPINGS[type])


def create_file(args, tmp_dir):
    geom_filter = ogr.CreateGeometryFromWkb(get_country_geometry(args.iso3))

    pq_type, pq_theme = args.type

    pq_path = f"{args.path}/theme={pq_theme}/type={pq_type}"
    pq_driver = ogr.GetDriverByName("parquet")
    pq_ds = pq_driver.Open(pq_path, 0)
    pq_layer = pq_ds.GetLayer()

    # Get input dataset schema
    defn = pq_layer.GetLayerDefn()

    file_name = f"{args.iso3.lower()}_{pq_theme}_{pq_type}.fgb"
    output_path = os.path.join(tmp_dir, file_name)
    output_driver = ogr.GetDriverByName("flatgeobuf")
    output_ds = output_driver.CreateDataSource(output_path)

    output_layer = output_ds.CreateLayer(
        pq_type,
        geom_type=pq_layer.GetGeomType(),
        srs=pq_layer.GetSpatialRef(),
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
    pq_layer.SetSpatialFilter(geom_filter)
    output_layer_defn = output_layer.GetLayerDefn()

    for feature in pq_layer:
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
        if counter % 1e4 == 0:
            print(f"Processed {counter} features")

        output_layer.CreateFeature(output_feature)

        output_feature = None
        counter += 1

    # Save and close DataSources

    pq_ds = None
    output_ds = None

    return output_path, file_name


def main():
    parser = ArgumentParser()

    parser.add_argument(
        "--iso3",
        dest="iso3",
        help="Country iso3 code",
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

    with TemporaryDirectory() as tmp_dir:
        output_path, file_name = create_file(args, tmp_dir)
        s3.put_file(output_path, os.path.join(BUCKET_NAME, file_name))


if __name__ == "__main__":
    main()
