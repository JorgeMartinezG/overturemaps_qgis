import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs

from qgis.core import (
    QgsFeature,
    QgsWkbTypes,
    QgsCoordinateReferenceSystem,
    QgsField,
    QgsFields,
    QgsFeatureSink,
    QgsGeometry,
)
from qgis.processing import alg
from qgis.PyQt.QtCore import QVariant

THEME_MAPS = [
    {
        "type": "building",
        "theme": "buildings",
        "geometry_type": QgsWkbTypes.Polygon,
    },
    {
        "type": "building_part",
        "theme": "buildings",
        "geometry_type": QgsWkbTypes.Polygon,
    },
    {
        "type": "segment",
        "theme": "transportation",
        "geometry_type": QgsWkbTypes.LineString,
    },
    {
        "type": "connector",
        "theme": "transportation",
        "geometry_type": QgsWkbTypes.LineString,
    },
]


def geoarrow_schema_adapter(schema: pa.Schema) -> pa.Schema:
    geometry_field_index = schema.get_field_index("geometry")
    geometry_field = schema.field(geometry_field_index)
    geoarrow_geometry_field = geometry_field.with_metadata(
        {b"ARROW:extension:name": b"geoarrow.wkb"}
    )

    geoarrow_schema = schema.set(geometry_field_index, geoarrow_geometry_field)

    return geoarrow_schema


FIELDS = [
    QgsField("id", QVariant.String),
    QgsField("version", QVariant.Int),
]


def row_to_feature(row, fields):
    geom = QgsGeometry()
    geom.fromWkb(row["geometry"])

    feat = QgsFeature()
    feat.setFields(fields)
    feat.setGeometry(geom)

    attributes = [row["id"], row["version"]]
    feat.setAttributes(attributes)

    return feat


@alg(
    name="OvertureMaps",
    label="Download data from overture maps",
    group="general",
    group_label="General",
)
@alg.input(type=alg.EXTENT, name="EXTENT", label="Area extent")
@alg.input(
    type=alg.ENUM,
    name="TYPE",
    label="Overture type",
    options=[i["type"] for i in THEME_MAPS],
)
@alg.input(type=alg.SINK, name="OUTPUT", label="overturemaps_layer")
def download_overture_maps(instance, parameters, context, feedback, inputs):
    """"""
    extent = instance.parameterAsExtent(parameters, "EXTENT", context)
    index = instance.parameterAsEnum(parameters, "TYPE", context)

    srid = instance.parameterAsExtentCrs(
        parameters, "EXTENT", context
    ).postgisSrid()
    if srid != 4326:
        feedback.reportError(f"Invalid srid: {srid}. Expected 4326")
        return {}

    theme_dict = THEME_MAPS[index]

    # Set attribute table.
    fields = QgsFields()
    [fields.append(f) for f in FIELDS]

    (sink, dest_id) = instance.parameterAsSink(
        parameters,
        "OUTPUT",
        context,
        fields,
        theme_dict["geometry_type"],
        QgsCoordinateReferenceSystem("EPSG:4326"),
    )

    bbox = extent.toRectF().getCoords()
    feedback.pushConsoleInfo(f"Using bbox = {bbox}")
    xmin, ymin, xmax, ymax = bbox

    filter = (
        (pc.field("bbox", "xmin") < xmax)
        & (pc.field("bbox", "xmax") > xmin)
        & (pc.field("bbox", "ymin") < ymax)
        & (pc.field("bbox", "ymax") > ymin)
    )

    theme = theme_dict["theme"]
    overture_type = theme_dict["type"]
    path = f"overturemaps-us-west-2/release/2024-08-20.0/theme={theme}/type={overture_type}/"
    dataset = ds.dataset(
        path, filesystem=fs.S3FileSystem(anonymous=True, region="us-west-2")
    )

    feedback.pushConsoleInfo("Fetching data...")
    batches = dataset.to_batches(filter=filter)
    non_empty_batches = (b for b in batches if b.num_rows > 0)

    geoarrow_schema = geoarrow_schema_adapter(dataset.schema)
    reader = pa.RecordBatchReader.from_batches(
        geoarrow_schema, non_empty_batches
    )

    feedback.pushConsoleInfo("Processing batches")
    counter = 1
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        if batch.num_rows == 0:
            continue
        feedback.pushConsoleInfo(f"Processing batch {counter}")
        features = [row_to_feature(row, fields) for row in batch.to_pylist()]
        for feature in features:
            if feature is None:
                feedback.pushCommandInfo("Skipping feature")
                continue

            sink.addFeature(feature, QgsFeatureSink.FastInsert)

        counter += 1

    return {"OUTPUT": dest_id}
