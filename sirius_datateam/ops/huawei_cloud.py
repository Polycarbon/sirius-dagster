import json
import os.path
import uuid
from typing import Tuple, List

import requests
from dagster import op, get_dagster_logger, DynamicOut, DynamicOutput, Failure, MetadataValue
from requests import Response


@op
def huawei_cloud_accounts() -> List[Tuple[str, str]]:
    accounts = [("hwc11429999", "oF3Zx2dQh3hKmmH"),
                ("hwc20469750", "i7pnJXV2mmy24WK"),
                ("hwc69656941", "C6ENPs7aiZTyGP8"),
                ("hwc55590589", "5QHQjeugqgHsKUW"),
                ("Sirius-Datateam", "Sy849uSGLtoNujt"),
                ("CGS-TRADE-PROD", "6ojMAmx5yXquxkzbTnP"),
                ("sirius-press", "jX25TMyEXhD8PQByfAk"),
                ("StarkCloud", "3Ch3F3kgGJnpaSGqhqj")]
    # why
    # accounts = [("hwc55590589", "5QHQjeugqgHsKUW")]
    return accounts


@op(out=DynamicOut(Response))
def get_token(accounts: List[Tuple[str, str]]) -> Response:
    """get api token by username password"""
    logger = get_dagster_logger()
    url_to_get_token = "https://iam.myhuaweicloud.com/v3/auth/tokens"
    headers_get_token = {'Content-Type': 'application/json;charset=utf8'}
    payload = {
        "auth": {
            "identity": {
                "methods": [
                    "password"
                ],
                "password": {
                    "user": {
                        "name": "sirius_api",
                        "password": "HWC_PASSWORD",
                        "domain": {
                            "name": "HWC_NAME"
                        }
                    }
                }
            },
            "scope": {
                "domain": {
                    "name": "HWC_NAME"
                }
            }
        }
    }
    for account in accounts:
        hwc_name, hwc_password = account

        payload = payload.__str__().replace("HWC_NAME", hwc_name)
        payload = payload.__str__().replace("HWC_PASSWORD", hwc_password)

        response = requests.post(url_to_get_token, headers=headers_get_token, data=payload)
        if response.ok:
            yield DynamicOutput(response, hwc_name.replace("-", "_"))
        else:
            logger.error(f"{hwc_name} : {response.text}")
            raise Failure(
                description=f"{response.text}",
                metadata={
                    "hwc_account": MetadataValue.text(hwc_name)
                },
            )


@op
def get_all_resources(token: Response) -> List[str]:
    """query all resources under huawei account"""
    headers_get_data = {
        "X-Auth-Token": "{0}".format(token.headers["X-Subject-Token"])
    }
    domain_id = token.json()["token"]["domain"]["id"]
    base_url = f"https://rms.myhuaweicloud.com/v1/resource-manager/domains/{domain_id}/all-resources"
    file_paths = []

    def resources_generator(marker="start"):
        if marker is None:
            return
        if marker == "start":
            url = base_url
        else:
            url = base_url + f"?marker={marker}"
        response = requests.get(url, headers=headers_get_data).json()
        resources = response["resources"]
        # count = response["page_info"]["current_count"]
        next_maker = response["page_info"]["next_marker"]
        key_idx = uuid.uuid4().hex
        path = f"{key_idx}.json"
        with open(path, "w") as file:
            json.dump(resources, file)
        yield path
        yield from resources_generator(next_maker)
        # return resources, next_maker

    for files in resources_generator():
        file_paths.append(files)
    return file_paths


@op
def to_json_file(context, hwc_resources):
    """write content to json"""
    logger = get_dagster_logger()
    _uuid = uuid.uuid4().hex
    with open(f"{_uuid}.json", "w") as file:
        json.dump(hwc_resources, file)
    logger.debug(_uuid)
    return f"{_uuid}.json"


@op(required_resource_keys={"s3"}, config_schema={"scheduled_date": str})
def upload_s3(context, files: List[str]) -> None:
    """upload content to s3"""
    logger = get_dagster_logger()
    s3_client = context.resources.s3
    scheduled_date = context.op_config["scheduled_date"]
    for file_name in files:
        object_name = os.path.basename(file_name)
        # Upload the file
        s3_client.upload_file(object_name, "sirius-dagster", f"hwc/resources/{scheduled_date}/{file_name}")


if __name__ == '__main__':
    obj = get_all_resources(get_token())
    result = next(obj)
    print(result)
