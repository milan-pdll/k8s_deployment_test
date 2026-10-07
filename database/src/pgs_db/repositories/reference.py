"""Read helpers over the gazetteer and domains for ETL stage 4 (geo-tagging).

The ETL spec (`ETL/spark/README.md` §4 stage 4) resolves a page to a place two ways:

- **Domain rules** -- a site that belongs to a local body (`pokharamun.gov.np`) tags
  every page it serves. `geo_for_domain` answers that from `domains.local_body_id`,
  and `link_domains_to_local_bodies` fills that column from the gazetteer's
  `local_bodies.website`.
- **Gazetteer matching** -- scan the text for any of the 837 place names.
  `gazetteer` exports them in one list for Spark to broadcast to its executors.

Both return codes in the shape `save_page`'s `geo_location` block expects.

It also serves the API's map and search result card: `hierarchy` for
`GET /api/v1/user/geo/hierarchy`, `regional_card` for a search's `regional_card`,
the curated `region_links` behind its quick links, and
`fill_local_body_contacts`, which fills a municipality's missing phone, email and
address from what its own website publishes.
"""

import json
import re
from typing import Any
from urllib.parse import urlsplit

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, joinedload

from ..enums import ContactType, GeoTagMethod
from ..models import (
    CrawledDocument,
    District,
    Domain,
    LocalBody,
    Page,
    PageContact,
    Province,
    RegionLink,
)

BOUNDARY_ATTRIBUTION = "Boundaries © Open Knowledge Nepal, CC BY 4.0"

# LocalBodyType -> the Nepali level word the UI's old municipality layer used.
_LEGACY_LEVEL = {
    "METROPOLITAN_CITY": "Mahanagarpalika",
    "SUB_METROPOLITAN_CITY": "Upamahanagarpalika",
    "MUNICIPALITY": "Nagarpalika",
    "RURAL_MUNICIPALITY": "Gaunpalika",
}


def site_host(url_or_host: str | None) -> str | None:
    """`http://www.pokharamun.gov.np/` or `WWW.Pokharamun.gov.np` -> `pokharamun.gov.np`."""
    if not url_or_host:
        return None
    value = url_or_host.strip().lower()
    host = urlsplit(value if "//" in value else f"//{value}").hostname
    if not host:
        return None
    return host.removeprefix("www.")


class ReferenceRepository:
    """Gazetteer and domain lookups for one SQLAlchemy session."""

    def __init__(self, session: Session) -> None:
        self.session = session

    def gazetteer(self) -> list[dict[str, Any]]:
        """Every province, district and local body with its names and parent codes.

        One flat list, provinces first, so Spark can broadcast it once and match
        `name_en` / `name_ne` in page text. Each entry carries the codes of every
        level above it, ready to become a `geo_location` block.
        """
        entries: list[dict[str, Any]] = []
        for p in self.session.scalars(select(Province).order_by(Province.code)):
            entries.append(
                {
                    "level": "province",
                    "code": p.code,
                    "name_en": p.name_en,
                    "name_ne": p.name_ne,
                    "province_code": p.code,
                    "district_code": None,
                    "municipality_id": None,
                }
            )
        for d in self.session.scalars(select(District).order_by(District.code)):
            entries.append(
                {
                    "level": "district",
                    "code": d.code,
                    "name_en": d.name_en,
                    "name_ne": d.name_ne,
                    "province_code": d.province_code,
                    "district_code": d.code,
                    "municipality_id": None,
                }
            )
        local_bodies = self.session.scalars(
            select(LocalBody).options(joinedload(LocalBody.district)).order_by(LocalBody.code)
        )
        for lb in local_bodies:
            entries.append(
                {
                    "level": "local_body",
                    "code": lb.code,
                    "name_en": lb.name_en,
                    "name_ne": lb.name_ne,
                    "type": lb.type,
                    "province_code": lb.district.province_code,
                    "district_code": lb.district_code,
                    "municipality_id": lb.code,
                }
            )
        return entries

    def geo_for_domain(self, domain_id: int | None) -> dict[str, Any] | None:
        """The `geo_location` block for a page on a local body's own site, or None.

        Returned with `method=DOMAIN` and `confidence=1.0`, so it can go straight into
        the `save_page` payload. None when the domain is unknown or not linked to a
        local body (news sites, national portals, ...).
        """
        if domain_id is None:
            return None
        local_body = self.session.scalar(
            select(LocalBody)
            .join(Domain, Domain.local_body_id == LocalBody.id)
            .where(Domain.id == domain_id)
            .options(joinedload(LocalBody.district))
        )
        if local_body is None:
            return None
        return {
            "province_code": local_body.district.province_code,
            "district_code": local_body.district_code,
            "municipality_id": local_body.code,
            "method": GeoTagMethod.DOMAIN.value,
            "confidence": 1.0,
        }

    def link_domains_to_local_bodies(self) -> int:
        """Set `domains.local_body_id` where the domain is a local body's website.

        Matches on host with `www.` ignored, and only fills rows that are still
        unlinked, so an admin's manual link is never overwritten. Safe to re-run
        after new domains are registered. Returns the number of domains linked.
        """
        by_host: dict[str, int] = {}
        for lb_id, website in self.session.execute(
            select(LocalBody.id, LocalBody.website).where(LocalBody.website.is_not(None))
        ):
            host = site_host(website)
            if host:
                by_host[host] = lb_id

        linked = 0
        unlinked = self.session.execute(
            select(Domain.id, Domain.domain).where(Domain.local_body_id.is_(None))
        ).all()
        for domain_id, domain in unlinked:
            lb_id = by_host.get(site_host(domain) or "")
            if lb_id is not None:
                self.session.execute(
                    update(Domain).where(Domain.id == domain_id).values(local_body_id=lb_id)
                )
                linked += 1
        return linked

    # ------------------------------------------------------------------ the map

    def hierarchy(self) -> list[dict[str, Any]]:
        """Provinces -> districts -> local bodies, ordered by code, for the map's tree."""
        local_bodies: dict[str, list[dict[str, Any]]] = {}
        for lb in self.session.scalars(select(LocalBody).order_by(LocalBody.code)):
            local_bodies.setdefault(lb.district_code, []).append(
                {"code": lb.code, "name_en": lb.name_en, "name_ne": lb.name_ne, "type": lb.type}
            )
        districts: dict[str, list[dict[str, Any]]] = {}
        for d in self.session.scalars(select(District).order_by(District.code)):
            districts.setdefault(d.province_code, []).append(
                {
                    "code": d.code,
                    "name_en": d.name_en,
                    "name_ne": d.name_ne,
                    "local_bodies": local_bodies.get(d.code, []),
                }
            )
        return [
            {
                "code": p.code,
                "name_en": p.name_en,
                "name_ne": p.name_ne,
                "districts": districts.get(p.code, []),
            }
            for p in self.session.scalars(select(Province).order_by(Province.code))
        ]

    # ------------------------------------------------------------ name lookup

    def codes_for_names(
        self,
        province: str | None = None,
        district: str | None = None,
        municipality: str | None = None,
    ) -> dict[str, str | None] | None:
        """Place names (English or Nepali) -> the most specific codes they resolve to.

        For payloads that carry names, like the ETL's `resolve_geo`
        (`{"province": "Gandaki Province", "district": "Kaski",
        "municipality": "Pokhara Metropolitan City"}`). Type words ("Province",
        "Metropolitan City", "Gaunpalika", ...) are ignored, and a municipality is only
        looked for inside the district (or province) already resolved, so a common
        name can never land in the wrong district. A unique prefix also matches
        ("Janakpur" -> "Janakpurdham"). Codes pass through (`P4`, `D38`, `MUN414`).
        Returns `{province_code, district_code, municipality_id}`, or None if nothing
        resolves.
        """
        province_code = self._match_one(Province, province)
        district_code = None
        if district:
            scope = [District.province_code == province_code] if province_code else []
            district_code = self._match_one(District, district, scope)
        if district_code:
            province_code = self.session.scalar(
                select(District.province_code).where(District.code == district_code)
            )
        local_body_code = None
        if municipality:
            if district_code:
                scope = [LocalBody.district_code == district_code]
            elif province_code:
                scope = [
                    LocalBody.district_code.in_(
                        select(District.code).where(District.province_code == province_code)
                    )
                ]
            else:
                scope = []
            local_body_code = self._match_one(LocalBody, municipality, scope)
        if local_body_code:
            district_code, province_code = self.session.execute(
                select(LocalBody.district_code, District.province_code)
                .join(District, District.code == LocalBody.district_code)
                .where(LocalBody.code == local_body_code)
            ).one()
        if not (province_code or district_code or local_body_code):
            return None
        return {
            "province_code": province_code,
            "district_code": district_code,
            "municipality_id": local_body_code,
        }

    def _match_one(
        self,
        model: type[Province] | type[District] | type[LocalBody],
        name: str | None,
        scope: list[Any] | None = None,
    ) -> str | None:
        """The one region of `model` in `scope` that `name` names, else None."""
        if not name or not name.strip():
            return None
        if self.session.scalar(select(model.code).where(model.code == name.strip())):
            return name.strip()
        wanted = _place_key(name)
        if not wanted:
            return None
        rows = self.session.execute(
            select(model.code, model.name_en, model.name_ne).where(*(scope or []))
        ).all()
        exact = {code for code, en, ne in rows if wanted in (_place_key(en), _place_key(ne))}
        if len(exact) == 1:
            return exact.pop()
        if exact:
            return None  # ambiguous
        prefixed = {
            code
            for code, en, _ in rows
            if (key := _place_key(en)) and (key.startswith(wanted) or wanted.startswith(key))
        }
        return prefixed.pop() if len(prefixed) == 1 else None

    # --------------------------------------------------------------- boundaries

    def locate(self, lat: float, lng: float) -> dict[str, str] | None:
        """The province, district and local body containing a WGS 84 point, or None.

        For pages that publish coordinates (Bronze `geo_lat` / `geo_lng`, geo meta tags):
        the result is a `geo_location` block without `method` / `confidence`, which the
        ETL sets (`GEO_META`). None outside Nepal, or before `seed_boundaries.py` ran.
        """
        if not (-90 <= lat <= 90 and -180 <= lng <= 180):
            raise ValueError(f"not a WGS 84 coordinate: lat={lat}, lng={lng}")
        point = func.ST_SetSRID(func.ST_MakePoint(lng, lat), 4326)
        row = self.session.execute(
            select(LocalBody.code, LocalBody.district_code, District.province_code)
            .join(District, District.code == LocalBody.district_code)
            .where(func.ST_Intersects(LocalBody.boundary, point))
            .order_by(LocalBody.code)
            .limit(1)
        ).one_or_none()
        if row is None:
            return None
        return {
            "province_code": row.province_code,
            "district_code": row.district_code,
            "municipality_id": row.code,
        }

    def boundaries_geojson(
        self,
        level: str,
        *,
        within: str | None = None,
        tolerance: float | None = None,
        legacy_properties: bool = False,
    ) -> dict[str, Any]:
        """A GeoJSON FeatureCollection of one level's shapes, for the UI's map.

        `level` is "province", "district" or "local_body"; `within` limits it to one
        parent (`P4` -> its districts, `D38` -> its local bodies). Shapes are simplified
        by `tolerance` degrees (default by level: ~1 km, ~500 m, ~100 m) with
        coordinates at 5 decimals, which keeps the whole country small enough to ship.
        Each feature's properties carry the code and names. Shapes are Open Knowledge
        Nepal's (CC BY 4.0): the map must credit them.

        `legacy_properties` also adds the property names the UI's old static GeoJSON
        used (`ADM1_PCODE` / `ADM1_EN`, `DISTRICT`, `NAME` / `DISTRICT` / `LEVEL` /
        `N_ID`), so these files replace `ui/public/data/nepal-*.geojson` with no UI
        code change (`scripts/export_boundaries.py`).
        """
        model: type[Province] | type[District] | type[LocalBody]
        if level == "province":
            model, parent, default_tolerance = Province, None, 0.01
        elif level == "district":
            model, parent, default_tolerance = District, District.province_code, 0.005
        elif level == "local_body":
            model, parent, default_tolerance = LocalBody, LocalBody.district_code, 0.001
        else:
            raise ValueError(f"level must be province, district or local_body, not {level!r}")
        shape = func.ST_AsGeoJSON(
            func.ST_SimplifyPreserveTopology(
                model.boundary, default_tolerance if tolerance is None else tolerance
            ),
            5,
        )
        stmt = select(model, shape).where(model.boundary.is_not(None)).order_by(model.code)
        if within is not None:
            if parent is None:
                raise ValueError("provinces have no parent to filter by")
            stmt = stmt.where(parent == within)
        district_names: dict[str, str] = {}
        if legacy_properties and level == "local_body":
            district_names = dict(
                self.session.execute(select(District.code, District.name_en)).all()
            )
        features = []
        for region, geometry in self.session.execute(stmt).all():
            properties: dict[str, Any] = {
                "code": region.code,
                "name_en": region.name_en,
                "name_ne": region.name_ne,
            }
            if isinstance(region, LocalBody):
                properties |= {"type": region.type.value, "district_code": region.district_code}
                if legacy_properties:
                    properties |= {
                        "NAME": region.name_en,
                        "DISTRICT": district_names.get(region.district_code),
                        "LEVEL": _LEGACY_LEVEL[region.type.value],
                        "N_ID": region.code,
                    }
            elif isinstance(region, District):
                properties["province_code"] = region.province_code
                if legacy_properties:
                    properties["DISTRICT"] = region.name_en.upper()
            elif legacy_properties:  # a province: P4 -> NP04, "4"
                number = region.code.removeprefix("P")
                properties |= {"ADM1_PCODE": f"NP{int(number):02d}", "ADM1_EN": number}
            features.append(
                {"type": "Feature", "properties": properties, "geometry": json.loads(geometry)}
            )
        return {
            "type": "FeatureCollection",
            "attribution": BOUNDARY_ATTRIBUTION,
            "features": features,
        }

    # ------------------------------------------------------------ regional card

    def regional_card(self, code: str) -> dict[str, Any] | None:
        """The search result's `regional_card` for a province, district or local body code.

        Quick links come from `region_links`: the region's own, then those of the
        regions above it (a municipality's card also lists its district's offices).
        Contact details exist only for local bodies. None for an unknown code.
        """
        lb = self.session.scalar(
            select(LocalBody)
            .where(LocalBody.code == code)
            .options(joinedload(LocalBody.district).joinedload(District.province))
        )
        if lb is not None:
            district, province = lb.district, lb.district.province
            return _card(
                "local_body",
                lb,
                local_body_type=lb.type,
                official_website=lb.website,
                contact={"phone": lb.phone, "email": lb.email, "address": lb.address},
                district=_named(district),
                province=_named(province),
                quick_links=self._links(
                    local_body_code=lb.code,
                    district_code=district.code,
                    province_code=province.code,
                ),
            )
        district = self.session.scalar(
            select(District).where(District.code == code).options(joinedload(District.province))
        )
        if district is not None:
            return _card(
                "district",
                district,
                province=_named(district.province),
                quick_links=self._links(
                    district_code=district.code, province_code=district.province_code
                ),
            )
        province = self.session.scalar(select(Province).where(Province.code == code))
        if province is None:
            return None
        return _card("province", province, quick_links=self._links(province_code=province.code))

    def _links(
        self,
        *,
        local_body_code: str | None = None,
        district_code: str | None = None,
        province_code: str | None = None,
    ) -> list[dict[str, Any]]:
        """The region's links, then its parents', each group by position."""
        links: list[dict[str, Any]] = []
        for column, value in (
            (RegionLink.local_body_code, local_body_code),
            (RegionLink.district_code, district_code),
            (RegionLink.province_code, province_code),
        ):
            if value is None:
                continue
            for link in self.session.scalars(
                select(RegionLink)
                .where(column == value)
                .order_by(RegionLink.position, RegionLink.id)
            ):
                links.append({"title": link.title_en, "title_ne": link.title_ne, "url": link.url})
        return links

    def add_region_link(
        self,
        code: str,
        *,
        title_en: str,
        url: str,
        title_ne: str | None = None,
        position: int = 0,
    ) -> RegionLink:
        """Add a quick link to a province, district or local body, by its code.

        Adding the same URL to the same region again updates its titles and position.
        """
        if not title_en.strip():
            raise ValueError("title_en must not be empty")
        stmt = insert(RegionLink).values(
            {
                self._region_column(code): code,
                "title_en": title_en.strip(),
                "title_ne": title_ne,
                "url": url.strip(),
                "position": position,
            }
        )
        link_id = self.session.scalar(
            stmt.on_conflict_do_update(
                constraint="uq_region_links_region_url",
                set_={
                    "title_en": stmt.excluded.title_en,
                    "title_ne": stmt.excluded.title_ne,
                    "position": stmt.excluded.position,
                    "updated_at": func.now(),
                },
            ).returning(RegionLink.id)
        )
        link = self.session.get(RegionLink, link_id)
        if link is None:  # pragma: no cover - RETURNING always yields the row
            raise RuntimeError(f"region link for {code!r} vanished after upsert")
        self.session.refresh(link)
        return link

    def remove_region_link(self, link_id: int) -> None:
        link = self.session.get(RegionLink, link_id)
        if link is None:
            raise LookupError(f"region link {link_id} does not exist")
        self.session.delete(link)

    def _region_column(self, code: str) -> str:
        for model, column in (
            (LocalBody, "local_body_code"),
            (District, "district_code"),
            (Province, "province_code"),
        ):
            if self.session.scalar(select(model.id).where(model.code == code)) is not None:
                return column
        raise LookupError(f"no province, district or local body has code {code!r}")

    # --------------------------------------------------------- contact backfill

    def fill_local_body_contacts(self) -> dict[str, int]:
        """Fill empty `local_bodies.phone` / `email` / `address` from each body's own site.

        Reads pages from domains linked to the local body, so run
        `link_domains_to_local_bodies` first. A municipality's contact block sits in
        its site footer, so the value found on the most pages wins. Only empty fields
        are filled, so curated values are never overwritten, and values too long for
        the column are skipped. Returns how many fields were filled, per field.
        """
        best = {
            "phone": self._most_common_contact(ContactType.PHONE, 50),
            "email": self._most_common_contact(ContactType.EMAIL, 255),
            "address": self._most_common_address(255),
        }
        filled = dict.fromkeys(best, 0)
        wanted = set().union(*best.values())
        if not wanted:
            return filled
        for lb in self.session.scalars(select(LocalBody).where(LocalBody.id.in_(wanted))):
            for field, values in best.items():
                value = values.get(lb.id)
                if value and getattr(lb, field) is None:
                    setattr(lb, field, value)
                    filled[field] += 1
        return filled

    def _most_common_contact(self, kind: ContactType, max_length: int) -> dict[int, str]:
        rows = self.session.execute(
            select(Domain.local_body_id, PageContact.value)
            .join(Page, Page.id == PageContact.page_id)
            .join(Domain, Domain.id == Page.domain_id)
            .where(
                Domain.local_body_id.is_not(None),
                PageContact.type == kind,
                Page.duplicate_of_id.is_(None),
                func.length(PageContact.value) <= max_length,
            )
            .group_by(Domain.local_body_id, PageContact.value)
            .order_by(
                Domain.local_body_id, func.count(func.distinct(Page.id)).desc(), PageContact.value
            )
        ).all()
        return _first_per_key(rows)

    def _most_common_address(self, max_length: int) -> dict[int, str]:
        address = func.btrim(CrawledDocument.address)
        rows = self.session.execute(
            select(Domain.local_body_id, address)
            .join(Domain, Domain.id == CrawledDocument.domain_id)
            .where(
                Domain.local_body_id.is_not(None),
                address != "",
                func.length(address) <= max_length,
            )
            .group_by(Domain.local_body_id, address)
            .order_by(
                Domain.local_body_id,
                func.count(func.distinct(CrawledDocument.normalized_url)).desc(),
                address,
            )
        ).all()
        return _first_per_key(rows)


# Type words that name the kind of region, not the region. Longest first.
_PLACE_TYPE_WORDS = (
    "sub-metropolitan city",
    "submetropolitan city",
    "metropolitan city",
    "rural municipality",
    "municipality",
    "upamahanagarpalika",
    "mahanagarpalika",
    "nagarpalika",
    "gaunpalika",
    "gaupalika",
    "province",
    "district",
    "उपमहानगरपालिका",
    "महानगरपालिका",
    "नगरपालिका",
    "गाउँपालिका",
    "प्रदेश",
    "जिल्ला",
)
# Spaces and punctuation only: \W would also strip Devanagari vowel signs.
# U+2019 (right single quotation mark) is a typographic apostrophe in place names.
_PLACE_SEPARATORS = re.compile(r"[\s\-_.,'\u2019()/]+")


def _place_key(name: str | None) -> str:
    """Lowercase, type words and punctuation removed: "Pokhara Metropolitan City" -> "pokhara"."""
    value = (name or "").strip().lower()
    for word in _PLACE_TYPE_WORDS:
        value = value.replace(word, " ")
    return _PLACE_SEPARATORS.sub("", value)


def _named(region: Province | District) -> dict[str, str]:
    return {"code": region.code, "name_en": region.name_en, "name_ne": region.name_ne}


def _card(level: str, region: Province | District | LocalBody, **fields: Any) -> dict[str, Any]:
    """A `RegionalCard`-shaped dict; fields that don't apply to the level stay None."""
    card: dict[str, Any] = {
        "level": level,
        "code": region.code,
        "region_name_en": region.name_en,
        "region_name_ne": region.name_ne,
        "local_body_type": None,
        "official_website": None,
        "contact": None,
        "district": None,
        "province": None,
        "quick_links": [],
    }
    card.update(fields)
    return card


def _first_per_key(rows: Any) -> dict[int, str]:
    """Rows come best-first within each key; keep the first value per key."""
    best: dict[int, str] = {}
    for key, value in rows:
        best.setdefault(key, value)
    return best
