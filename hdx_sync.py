import os
import s3fs
from argparse import ArgumentParser
from datetime import datetime
from pathlib import Path
from typing import TypedDict, List, cast, Tuple
from utils import AWS_BUCKET_NAME, AWS_REGION, Boundary, get_boundaries
from hdx.api.configuration import Configuration  # type: ignore
from hdx.data.dataset import Dataset  # type: ignore

MARKDOWN = """
This dataset is an extraction of segments and buildings from  OvertureMaps database for use in GIS applications.
The data is updated per release and include all latest updates. \n
"""


class OvertureItem(TypedDict):
    object_id: int
    theme: str
    type: str
    release: str
    file_path: str


class Resource(TypedDict):
    name: str
    format: str
    description: str
    url: str
    last_modified: str


def parse_object(s3_object: str) -> OvertureItem:
    file_path = Path(s3_object).name
    file_name = file_path.split(".")[0]

    _, object_id, theme, type, release_str = file_name.split("_")

    date_release = (
        datetime.strptime(release_str[:-1], "%Y%m%d").date().isoformat()
    )
    release = f"{date_release}.{release_str[-1]}"

    item: OvertureItem = {
        "object_id": int(object_id),
        "theme": theme,
        "type": type,
        "release": release,
        "file_path": file_path,
    }

    return item


def create_overtureitems(s3_objects: List[str]) -> List[OvertureItem]:
    return [parse_object(obj) for obj in s3_objects]


def item_to_hdx_resource(
    item: OvertureItem, boundaries: List[Boundary]
) -> Resource:
    match_boundary = next(
        (b for b in boundaries if b.id == item["object_id"]), None
    )
    if match_boundary is None:
        raise ValueError(f"No boundary match found for {item['object_id']}")

    title = f"{match_boundary.name} {item['type']} extract"

    resource: Resource = {
        "name": title,
        "format": "QGIS",
        "description": "flatgeobuf file",
        "url": f"https://{AWS_BUCKET_NAME}.s3.{AWS_REGION}.amazonaws.com/{item['file_path']}",
        "last_modified": datetime.now().isoformat(),
    }

    return resource


def get_resources_from_s3() -> Tuple[List[Resource], List[str]]:
    s3 = s3fs.S3FileSystem(
        anon=False,
        client_kwargs={"region_name": AWS_REGION},
    )

    s3_objects = s3.ls(AWS_BUCKET_NAME)
    items = create_overtureitems(s3_objects)

    object_ids = list(set([i["object_id"] for i in items]))
    boundaries = get_boundaries(object_ids, with_geom=False)

    resources = [item_to_hdx_resource(i, boundaries) for i in items]

    sorted_items: List[Resource] = sorted(resources, key=lambda x: x["name"])

    iso3_codes = [b.iso3 for b in boundaries]

    return sorted_items, iso3_codes


def update_dataset(dataset: Dataset, resources: List[Resource]) -> None:
    old_resources = Dataset.get_all_resources([dataset])
    [dataset.delete_resource(r) for r in old_resources]

    dataset.add_update_resources(resources)  # type: ignore
    dataset.update_in_hdx()


def create_dataset(
    ds_name: str, resources: List[Resource], iso3_codes: List[str]
) -> str:
    ds_title = "Overture Maps extracts by country"

    iso3_codes_dict = [{"name": i.lower()} for i in iso3_codes]

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
            },
            {
                "name": "roads",
            },
            {
                "name": "transportation",
            },
            {
                "name": "facilities-infrastructure",
            },
        ],
        "groups": iso3_codes_dict,
        "private": False,
        "notes": MARKDOWN,
    }

    dataset = Dataset(metadata_draft)
    dataset.set_expected_update_frequency("Never")
    dataset.set_reference_period("2023-07-26", "2024-08-20")
    dataset.add_update_resources(resources)  # type: ignore
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

    resources, iso3_codes = get_resources_from_s3()
    ds_name = "overturemaps_extracts_wfp"
    dataset = Dataset.read_from_hdx(ds_name)

    if dataset is None:
        create_dataset(ds_name, resources, iso3_codes)
    else:
        update_dataset(cast(Dataset, dataset), resources)


if __name__ == "__main__":
    main()
