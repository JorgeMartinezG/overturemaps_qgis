import os
import s3fs
from argparse import ArgumentParser
from datetime import datetime
from osgeo import ogr
from pathlib import Path
from typing import TypedDict, Optional, List
from utils import AWS_BUCKET_NAME, AWS_REGION
from hdx.api.configuration import Configuration  # type: ignore
from hdx.data.dataset import Dataset  # type: ignore


MARKDOWN = """
This dataset is an extraction of segments and buildings from  OvertureMaps database for use in GIS applications.
The data is updated per release and include all latest updates. \n
"""


class OvertureItem(TypedDict):
    iso3: str
    theme: str
    type: str
    release: str
    adm_name: Optional[str]
    file_path: str


class Resource(TypedDict):
    name: str
    format: str
    description: str
    url: str
    last_modified: str


class FileItem(TypedDict):
    overtureitem: OvertureItem
    hdx_resource: Resource


def parse_object(s3_object: str) -> OvertureItem:
    file_path = Path(s3_object).name
    file_name = file_path.split(".")[0]

    iso3, theme, type, release_str = file_name.split("_")

    date_release = (
        datetime.strptime(release_str[:-1], "%Y%m%d").date().isoformat()
    )
    release = f"{date_release}.{release_str[-1]}"

    item: OvertureItem = {
        "iso3": iso3,
        "theme": theme,
        "type": type,
        "release": release,
        "file_path": file_path,
        "adm_name": None,
    }

    return item


def get_country_names(items: List[OvertureItem]) -> List[OvertureItem]:
    iso3_codes = [i["iso3"].upper() for i in items]

    # Read shapefile layer.
    boundaries_drv = ogr.GetDriverByName("flatgeobuf")
    boundaries_ds = boundaries_drv.Open("./boundaries.fgb", 0)
    if boundaries_ds is None:
        raise ValueError("Boundaries file missing")

    boundaries_lyr = boundaries_ds.GetLayer()

    filter_str = ", ".join([f"'{i}'" for i in iso3_codes])
    filter_str = f"iso3 IN ({filter_str})"

    boundaries_lyr.SetAttributeFilter(filter_str)

    items_with_country = []
    for feature in boundaries_lyr:
        name = feature["adm0_name"]
        iso3 = feature["iso3"]

        # Find matching item in the dictionary.
        overture_items = [i for i in items if i["iso3"] == iso3.lower()]
        if len(overture_items) == 0:
            raise ValueError(f"iso3 code not found {iso3}")

        overture_items_with_name: List[OvertureItem] = [
            {
                **item,
                "adm_name": name,
            }
            for item in overture_items
        ]

        items_with_country.extend(overture_items_with_name)

    return items_with_country


def create_overtureitems(s3_objects: List[str]) -> List[OvertureItem]:
    overture_items = [parse_object(obj) for obj in s3_objects]

    items_with_country_name = get_country_names(overture_items)
    return items_with_country_name


def item_to_resource(item: OvertureItem) -> FileItem:
    title = f"{item['adm_name']} {item['type']} extract"

    resource: Resource = {
        "name": title,
        "format": "QGIS",
        "description": "flatgeobuf file",
        "url": f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{item['file_path']}",
        "last_modified": datetime.now().isoformat(),
    }

    return {"hdx_resource": resource, "overtureitem": item}


def items_to_hdx_resources(items: List[OvertureItem]) -> List[FileItem]:
    return [item_to_resource(item) for item in items]


def get_resources_from_s3() -> List[FileItem]:
    s3 = s3fs.S3FileSystem(
        anon=False,
        client_kwargs={"region_name": AWS_REGION},
    )

    s3_objects = s3.ls(AWS_BUCKET_NAME)
    items = create_overtureitems(s3_objects)
    resources = items_to_hdx_resources(items)

    return resources


def create_or_update_dataset(items: List[FileItem]) -> str:
    ds_name = "overturemaps_extracts_wfp"
    ds_title = "Overture Maps extracts by country"

    iso3_codes = [{"name": i["overtureitem"]["iso3"].lower()} for i in items]
    resources = [i["hdx_resource"] for i in items]

    metadata_draft = {
        "name": ds_name,
        "title": ds_title,
        "owner_org": os.getenv("HDX_ORG"),
        "maintainer": os.getenv("HDX_USER"),
        "dataset_source": "OvertureMaps foundation",
        "methodology": "Other",
        "methodology_other": "Volunteered geographic information",
        "license_id": "hdx-odc-odbl",
        "tags": [
            {
                "name": "geodata",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "roads",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
            {
                "name": "transportation",
                "vocabulary_id": "b891512e-9516-4bf5-962a-7a289772a2a1",
            },
        ],
        "groups": iso3_codes,
        "private": False,
        "notes": MARKDOWN,
    }

    dataset = Dataset(metadata_draft)
    dataset.set_expected_update_frequency("Never")
    dataset.set_reference_period("2023-07-26", "2024-08-20")
    dataset.add_update_resources(resources)
    dataset.create_in_hdx()

    return ds_name


def main():
    parser = ArgumentParser("OvertureMaps sync HDX")
    parser.add_argument("-s", "--staging", action="store_true")

    args = parser.parse_args()

    Configuration.create(
        hdx_site="prod" if args.staging is False else "stage",
        hdx_key=os.getenv("HDX_KEY"),
        user_agent="wfp_osm",
    )

    file_items = get_resources_from_s3()

    dataset_id = create_or_update_dataset(file_items)


if __name__ == "__main__":
    main()
