import os
from dataclasses import dataclass
from pathlib import Path

ARCGIS = "https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services"
DAILY_PORTS = f"{ARCGIS}/Daily_Ports_Data/FeatureServer/0/query"
DAILY_CHOKEPOINTS = f"{ARCGIS}/Daily_Chokepoints_Data/FeatureServer/0/query"
CHOKEPOINTS = f"{ARCGIS}/PortWatch_chokepoints_database/FeatureServer/0/query"
PORTS = f"{ARCGIS}/PortWatch_ports_database/FeatureServer/0/query"
DISRUPTIONS = f"{ARCGIS}/portwatch_disruptions_database/FeatureServer/0/query"
GSCPI_URL = "https://www.newyorkfed.org/medialibrary/research/interactives/gscpi/downloads/gscpi_data.xlsx"

DAILY_FIELDS = [
    "date", "portid", "portcalls_container", "portcalls_dry_bulk", "portcalls_general_cargo",
    "portcalls_roro", "portcalls_tanker", "portcalls", "import", "export",
]
PAGE_SIZE = 1000
WORKERS = 10
# PortWatch revises recent days, so incremental runs re-read this many days before the watermark
REVISION_DAYS = 21


@dataclass(frozen=True)
class Paths:
    root: Path

    @property
    def bronze(self) -> Path:
        return self.root / "bronze"

    @property
    def calls(self) -> Path:
        return self.bronze / "port_calls"

    @property
    def warehouse(self) -> Path:
        return self.root / "warehouse.duckdb"

    @property
    def reports(self) -> Path:
        return self.root / "reports"


def default_paths() -> Paths:
    return Paths(Path(os.environ.get("ATLAS_DATA_DIR", "data")))
