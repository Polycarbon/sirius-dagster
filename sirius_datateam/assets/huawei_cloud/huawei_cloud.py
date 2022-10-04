import json
import os
import uuid

import requests
from dagster import asset, get_dagster_logger, op, job


@asset
def get_token_a():
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
                        "password": "Sy849uSGLtoNujt",
                        "domain": {
                            "name": "Sirius-Datateam"
                        }
                    }
                }
            },
            "scope": {
                "domain": {
                    "name": "Sirius-Datateam"
                }
            }
        }
    }
    # payload = payload.replace("hwc_pass_replace", "Sirius-Datateam")
    # payload = payload.replace("hwc_acc_replace", "Sy849uSGLtoNujt")

    return requests.post(url_to_get_token, headers=headers_get_token, data=payload.__str__())


@asset
def get_all_resources_a(get_token_a):
    headers_get_data = {
        "X-Auth-Token": "{0}".format(get_token_a.headers["X-Subject-Token"])
    }
    domain_id = get_token_a.json()["token"]["domain"]["id"]
    url_path_get_data = f"https://rms.myhuaweicloud.com/v1/resource-manager/domains/{domain_id}/all-resources"
    response = requests.get(url_path_get_data, headers=headers_get_data)
    resources = response.json()["resources"]

    return resources


@asset
def to_json_a(get_all_resources_a):
    logger = get_dagster_logger()
    _uuid = uuid.uuid4().hex
    with open(f"resources.json", "w") as file:
        json.dump(get_all_resources_a, file)
    return f"resources.json"


@asset(required_resource_keys={"s3"})
def upload_s3_a(context, to_json_a):
    logger = get_dagster_logger()
    context.resources.s3

    object_name = os.path.basename(to_json_a)

        # Upload the file
    s3_client = context.resources.s3
    try:
        response = s3_client.upload_file("resources.json", "storage", object_name)
        logger.debug(response)
    except Exception as e:
        logger.error(e)
        return False
    return True

# @job
# def hwc_resource_ingest():
#     get_all_resources(get_token())

#
# if __name__ == '__main__':
#     res = get_token()
#
#     data = get_all_resources(res)
#     print(res)
