"""Bootstrap the course Polaris catalog and its dev/analyst role chain."""

import json
import os
from pathlib import Path
import shlex
import urllib.error
import urllib.parse
import urllib.request


BASE = os.environ.get("POLARIS_URI", "http://polaris:8181").rstrip("/")
API = BASE + "/api/management/v1"
REALM = "POLARIS"


def request(method: str, url: str, token: str | None = None, body: dict | None = None):
    payload = None if body is None else json.dumps(body).encode()
    headers = {"Polaris-Realm": REALM, "Accept": "application/json"}
    if token:
        headers["Authorization"] = "Bearer " + token
    if payload is not None:
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=payload, method=method, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=30) as response:
            raw = response.read()
            return response.status, json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        if exc.code == 404 and method == "GET":
            return 404, {}
        raise RuntimeError(f"Polaris {method} {url}: HTTP {exc.code}: {exc.read()[:500]!r}") from exc


def get_token() -> str:
    secret = os.environ["POLARIS_ROOT_SECRET"]
    form = urllib.parse.urlencode({
        "grant_type": "client_credentials", "scope": "PRINCIPAL_ROLE:ALL"
    }).encode()
    auth = ("root:" + secret).encode()
    import base64
    req = urllib.request.Request(
        BASE + "/api/catalog/v1/oauth/tokens", data=form, method="POST",
        headers={
            "Authorization": "Basic " + base64.b64encode(auth).decode(),
            "Polaris-Realm": REALM,
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.load(response)["access_token"]


def ensure(token: str, path: str, collection: str, payload: dict):
    status, _ = request("GET", API + path, token)
    if status == 404:
        _, result = request("POST", API + collection, token, payload)
        return result
    return None


def grant(token: str, role: str, privilege: str, catalog: str):
    path = f"/catalogs/{catalog}/catalog-roles/{role}/grants"
    _, result = request("GET", API + path, token)
    if any(g.get("type") == "catalog" and g.get("privilege") == privilege
           for g in result.get("grants", [])):
        return
    request("PUT", API + path, token,
            {"grant": {"type": "catalog", "privilege": privilege}})


def assign(token: str, path: str, result_key: str, name: str, payload_key: str):
    _, result = request("GET", API + path, token)
    if any(role.get("name") == name for role in result.get(result_key, [])):
        return
    request("PUT", API + path, token, {payload_key: {"name": name}})


def save_credential(name: str, value: str) -> Path:
    destination = Path.home() / ".polaris-credentials.env"
    existing = destination.read_text() if destination.exists() else ""
    key = f"POLARIS_{name.upper()}_CREDENTIAL"
    lines = [line for line in existing.splitlines() if not line.startswith(key + "=")]
    lines.append(f"{key}={shlex.quote(value)}")
    destination.touch(mode=0o600, exist_ok=True)
    destination.chmod(0o600)
    destination.write_text("\n".join(lines) + "\n")
    return destination


def main():
    token = get_token()
    catalog = os.environ["POLARIS_CATALOG"]
    bucket = os.environ["LAB_BUCKET_NAME"]
    endpoint = os.environ["POLARIS_S3_ENDPOINT"]
    new_credentials = []
    for name in ("dev", "analyst"):
        created = ensure(token, f"/principals/{name}", "/principals",
                         {"principal": {"name": name, "properties": {}},
                          "credentialRotationRequired": False})
        if created:
            item = created["credentials"]
            location = save_credential(name, item["clientId"] + ":" + item["clientSecret"])
            new_credentials.append(name)
            print(f"Saved {name} credential to {location}")
    for role in ("data_engineer", "data_analyst"):
        ensure(token, f"/principal-roles/{role}", "/principal-roles",
               {"principalRole": {"name": role, "properties": {}}})

    location = f"s3://{bucket}/warehouse/"
    ensure(token, f"/catalogs/{catalog}", "/catalogs", {"catalog": {
        "name": catalog, "type": "INTERNAL",
        "properties": {"default-base-location": location},
        "storageConfigInfo": {
            "storageType": "S3", "allowedLocations": [location],
            "endpoint": endpoint, "endpointInternal": endpoint,
            "pathStyleAccess": True, "region": os.environ.get("AWS_REGION", "us-east-1"),
            "stsUnavailable": True,
        },
    }})
    for role in ("catalog_contributor", "catalog_reader"):
        ensure(token, f"/catalogs/{catalog}/catalog-roles/{role}",
               f"/catalogs/{catalog}/catalog-roles",
               {"catalogRole": {"name": role, "properties": {}}})
    for privilege in ("CATALOG_MANAGE_ACCESS", "CATALOG_MANAGE_METADATA", "CATALOG_MANAGE_CONTENT"):
        grant(token, "catalog_contributor", privilege, catalog)
    for privilege in ("TABLE_READ_DATA", "TABLE_READ_PROPERTIES", "CATALOG_READ_PROPERTIES"):
        grant(token, "catalog_reader", privilege, catalog)
    assign(token, "/principals/dev/principal-roles", "roles", "data_engineer", "principalRole")
    assign(token, "/principals/analyst/principal-roles", "roles", "data_analyst", "principalRole")
    assign(token, f"/principal-roles/data_engineer/catalog-roles/{catalog}",
           "roles", "catalog_contributor", "catalogRole")
    assign(token, f"/principal-roles/data_analyst/catalog-roles/{catalog}",
           "roles", "catalog_reader", "catalogRole")

    if not new_credentials and not (Path.home() / ".polaris-credentials.env").exists():
        print("Principals already exist; retrieve their credentials from the previous secure record or rotate them.")
    print(f"Polaris bootstrap complete: {catalog}, dev and analyst role chains")


if __name__ == "__main__":
    main()
