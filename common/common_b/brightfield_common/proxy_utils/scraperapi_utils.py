"""Utility functions for ScraperAPI proxy."""

from brightfield_common.google_utils.secrets_utils import access_secret_version


def get_scraperapi_credentials() -> str:
    """Attempts to grab ScraperAPI credentials from Google Secrets Manager."""
    return access_secret_version("344678106133", "scraperapi-key")
