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
            url = base_url+f"?marker={marker}"
        response = requests.get(url, headers=headers_get_data).json()
        resources = response["resources"]
        # count = response["page_info"]["current_count"]
        next_maker = response["page_info"]["next_marker"]
        yield DynamicOutput(resources, uuid.uuid4().hex)
        yield from resources_generator(next_maker)
        # return resources, next_maker
    return resources_generator()

@op
def write_s3(out_resources):
    """write content to s3"""
    logger = get_dagster_logger()
    logger.debug(out_resources)
    logger.debug(len(out_resources))
    return len(out_resources)



if __name__ == '__main__':
    obj = get_all_resources(get_token())
    result = obj.map(write_s3)
    print(result)
