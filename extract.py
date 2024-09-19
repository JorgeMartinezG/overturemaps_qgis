from osgeo import ogr, osr


def main():
    # Read shapefile layer.
    shp_path = "/Users/gis/data/boundaries/wld_bnd_adm0_ge"
    shp_driver = ogr.GetDriverByName("ESRI Shapefile")
    shp_ds = shp_driver.Open(shp_path, 0)
    boundaries_layer = shp_ds.GetLayer()
    boundaries_layer.SetAttributeFilter("iso3 = 'LSO'")

    country_boundary = next((f for f in boundaries_layer), None)
    country_geom = country_boundary.geometry()

    buildings_path = (
        "/Users/gis/data/om_buildings/theme=buildings/type=building"
    )
    pq_driver = ogr.GetDriverByName("parquet")
    dataSource = pq_driver.Open(buildings_path, 0)
    buildings_layer = dataSource.GetLayer()
    buildings_defn = buildings_layer.GetLayerDefn()

    output_path = "/Users/gis/data/lesotho.fgb"
    out_driver = ogr.GetDriverByName("FlatGeoBuf")
    output_ds = out_driver.CreateDataSource(output_path)

    srs = osr.SpatialReference()
    srs.ImportFromEPSG(4326)
    output_layer = output_ds.CreateLayer(
        "buildings",
        geom_type=ogr.wkbMultiPolygon,
        srs=srs,
        options=["SPATIAL_INDEX=YES"],
    )

    for i in range(0, buildings_defn.GetFieldCount()):
        field_defn = buildings_defn.GetFieldDefn(i)
        output_layer.CreateField(field_defn)

    buildings_layer.SetSpatialFilter(country_geom)

    output_defn = output_layer.GetLayerDefn()
    print("Processing features")
    # for count in range(0, buildings_layer.GetFeatureCount()):
    for inFeature in buildings_layer:
        # inFeature = buildings_layer.GetFeature(count)
        # Create output Feature
        outFeature = ogr.Feature(output_defn)
        # Add field values from input Layer
        for i in range(0, output_defn.GetFieldCount()):
            outFeature.SetField(
                output_defn.GetFieldDefn(i).GetNameRef(), inFeature.GetField(i)
            )
        # Set geometry as centroid
        outFeature.SetGeometry(inFeature.GetGeometryRef())
        # Add new feature to output Layer
        output_layer.CreateFeature(outFeature)

    inFeature = None
    outFeature = None
    # Save and close DataSources
    shp_ds = None
    dataSource = None
    output_ds = None


if __name__ == "__main__":
    main()
