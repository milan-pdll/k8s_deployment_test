"""Write the map's GeoJSON files from the database, ready to drop into the UI.

Replaces `ui/public/data/nepal-{provinces,districts,municipalities}.geojson`, which
predate the 2017 restructuring (75 districts; 544 of 766 municipality names match
the gazetteer). The files keep the property names the UI already reads (`ADM1_PCODE`,
`DISTRICT`, `NAME`, `LEVEL`, ...) and add ours (`code`, `name_en`, `name_ne`), so the
map works unchanged today and can switch to codes when the API serves search counts.

Run after `seed_boundaries.py`. The shapes are Open Knowledge Nepal's (CC BY 4.0):
the map must show "Boundaries © Open Knowledge Nepal, CC BY 4.0".

Usage:
    python scripts/export_boundaries.py                      # -> ./boundaries-export/
    python scripts/export_boundaries.py ../ui/public/data    # straight into the UI
"""

import argparse
import json
from pathlib import Path

from pgs_db import ReferenceRepository, make_session_factory

# level -> the UI's file name
FILES = {
    "province": "nepal-provinces.geojson",
    "district": "nepal-districts.geojson",
    "local_body": "nepal-municipalities.geojson",
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("out_dir", nargs="?", default="boundaries-export")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=None,
        help="simplification in degrees (default: per level)",
    )
    args = parser.parse_args()
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    with make_session_factory()() as session:
        reference = ReferenceRepository(session)
        for level, filename in FILES.items():
            collection = reference.boundaries_geojson(
                level, tolerance=args.tolerance, legacy_properties=True
            )
            if not collection["features"]:
                raise SystemExit("no boundaries in the database: run seed_boundaries.py first")
            path = out / filename
            path.write_text(json.dumps(collection, separators=(",", ":")), encoding="utf-8")
            size_kb = path.stat().st_size // 1024
            print(f"{path}: {len(collection['features'])} features, {size_kb} KB")


if __name__ == "__main__":
    main()
