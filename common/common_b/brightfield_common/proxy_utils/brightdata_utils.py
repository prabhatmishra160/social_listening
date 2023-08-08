"""Utility functions for Brightdata proxy."""

import requests
from brightfield_common.google_utils.secrets_utils import access_secret_version


def get_brightdata_api_token() -> str:
    """Attempts to grab Brightdata `api_token` from Google Secrets Manager."""
    return access_secret_version("344678106133", "brightdata-api-token")


def get_brightdata_account_id() -> str:
    """Attempts to grab Brightdata account ID from Google Secrets Manager."""
    return access_secret_version("344678106133", "brightdata-username")


def get_brightdata_active_zones(api_token=None) -> list:
    """Attempts to grab all available Brightdata zones from Brightdata API.

    Passing `api_token` is optional. If left `None`, will automatically
    call `brightfield_common.google_utils.secrets.get_brightdata_api_token`
    to fetch the token.

    Returns list of dicts of zones available.
    """
    url = "https://brightdata.com/api/zone/get_active_zones"

    if api_token is None:
        api_token = get_brightdata_api_token()

    headers = {
        "Authorization": f"Bearer {api_token}"
    }

    r = requests.get(url, headers=headers)

    if r.status_code == 200:
        r_json = r.json()
        return r_json
    elif r.status_code == 422:
        # zone doesnt exist, return empty list
        return []

    raise Exception(
        f"Request to fetch zone returned unexpected response ({r.status_code}): {r}"  # noqa
    )


def get_brightdata_zone_password(zone_name, api_token=None) -> list:
    """Attempts to grab Brightdata zone passwords from Brightdata API.

    Passing `api_token` is optional. If left `None`, will automatically
    call `brightfield_common.google_utils.secrets.get_brightdata_api_token`
    to fetch the token.

    Returns `list` because Brightdata zones can have more than 1 password.
    """
    url = f"https://luminati.io/api/zone/passwords?zone={zone_name}"

    if api_token is None:
        api_token = get_brightdata_api_token()

    headers = {
        "Authorization": f"Bearer {api_token}"
    }

    r = requests.get(url, headers=headers)

    if r.status_code == 200:
        r_json = r.json()
        if "passwords" in r_json.keys():
            return r_json["passwords"]
    elif r.status_code == 422:
        # zone doesnt exist, return empty list
        return []

    raise Exception(
        f"Request to fetch zone returned unexpected response ({r.status_code}): {r}"  # noqa
    )
