from dataclasses import dataclass
import requests
from om_lineage_cli.models import TableEntity


@dataclass
class OpenMetadataClient:
    base_url: str
    token: str

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def get_table(self, fqn: str) -> TableEntity:
        url = f"{self.base_url}/api/v1/tables/name/{fqn}"
        resp = requests.get(url, headers=self._headers(), timeout=10)
        resp.raise_for_status()
        data = resp.json()
        columns = {
            c["name"]: f"{data['fullyQualifiedName']}.{c['name']}" for c in data.get("columns", [])
        }
        return TableEntity(id=data["id"], fqn=data["fullyQualifiedName"], columns=columns)

    def post_lineage(self, payload: dict) -> None:
        url = f"{self.base_url}/api/v1/lineage"
        resp = requests.post(url, headers=self._headers(), json=payload, timeout=10)
        resp.raise_for_status()
