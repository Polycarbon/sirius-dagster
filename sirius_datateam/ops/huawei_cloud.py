import json
import os.path
import uuid

import requests
from dagster import op, get_dagster_logger, DynamicOut, DynamicOutput


@op
def get_token():
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
                        "password": "oF3Zx2dQh3hKmmH",
                        "domain": {
                            "name": "hwc11429999"
                        }
                    }
                }
            },
            "scope": {
                "domain": {
                    "name": "hwc11429999"
                }
            }
        }
    }
    # payload = payload.replace("hwc_pass_replace", "Sirius-Datateam")
    # payload = payload.replace("hwc_acc_replace", "Sy849uSGLtoNujt")

    return requests.post(url_to_get_token, headers=headers_get_token, data=payload.__str__())


@op(out=DynamicOut())
def get_all_resources(token):
    """query all resources under huawei account"""
    headers_get_data = {
        "X-Auth-Token": "{0}".format(token.headers["X-Subject-Token"])
    }
    domain_id = token.json()["token"]["domain"]["id"]
    base_url = f"https://rms.myhuaweicloud.com/v1/resource-manager/domains/{domain_id}/all-resources"

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
        yield DynamicOutput(resources, uuid.uuid4().hex)
        yield from resources_generator(next_maker)
        # return resources, next_maker

    return resources_generator()


@op
def to_json_file(context, hwc_resources):
    """write content to json"""
    logger = get_dagster_logger()
    _uuid = uuid.uuid4().hex
    with open(f"{_uuid}.json", "w") as file:
        json.dump(hwc_resources, file)
    logger.debug(_uuid)
    return f"{_uuid}.json"


@op(required_resource_keys={"s3"})
def upload_s3(context, file_name):
    """upload content to s3"""
    logger = get_dagster_logger()
    object_name = os.path.basename(file_name)
    # Upload the file
    s3_client = context.resources.s3
    response = s3_client.upload_file(f"{file_name}", "sirius-dagster", object_name)
    logger.debug(response)


if __name__ == '__main__':
    obj = get_all_resources(get_token())
    result = next(obj)
    print(result)
