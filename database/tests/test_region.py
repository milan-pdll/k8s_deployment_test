"""The map tree, the regional card, region_links and the local-body contact backfill."""

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from pgs_db import BronzeRepository, ReferenceRepository, SilverRepository
from pgs_db.models import Domain, LocalBody, RegionLink
from pgs_db.schemas import ProvinceNode, RegionalCard

# Seeded gazetteer: Pokhara (MUN414) is in Kaski (D38), Gandaki (P4).
POKHARA, KASKI, GANDAKI = "MUN414", "D38", "P4"
SITE = "pokhara-test.gov.np"
_ANCIENT = datetime(1975, 1, 1, tzinfo=UTC)


@pytest.fixture()
def ref(session: Session) -> ReferenceRepository:
    return ReferenceRepository(session)


def pokhara(ref: ReferenceRepository) -> LocalBody:
    lb = ref.session.scalar(select(LocalBody).where(LocalBody.code == POKHARA))
    assert lb is not None
    return lb


class TestHierarchy:
    def test_every_level_is_present_and_nested(self, ref: ReferenceRepository) -> None:
        tree = [ProvinceNode.model_validate(p) for p in ref.hierarchy()]
        assert [p.code for p in tree] == [f"P{i}" for i in range(1, 8)]
        assert sum(len(p.districts) for p in tree) == 77
        assert sum(len(d.local_bodies) for p in tree for d in p.districts) == 753
        gandaki = next(p for p in tree if p.code == GANDAKI)
        kaski = next(d for d in gandaki.districts if d.code == KASKI)
        assert POKHARA in [lb.code for lb in kaski.local_bodies]


class TestRegionalCard:
    def test_a_local_body_card_has_contacts_parents_and_links_in_order(
        self, ref: ReferenceRepository
    ) -> None:
        lb = pokhara(ref)
        lb.phone, lb.email, lb.address = "+977-61-521105", "info@pokharamun.gov.np", "New Road"
        ref.add_region_link(KASKI, title_en="DAO Kaski", url="https://daokaski.moha.gov.np")
        ref.add_region_link(
            POKHARA, title_en="Ward Directives", url="https://x.gov.np/w", position=2
        )
        ref.add_region_link(POKHARA, title_en="Budget", url="https://x.gov.np/b", position=1)

        card = RegionalCard.model_validate(ref.regional_card(POKHARA))

        assert (card.level, card.region_name_en) == ("local_body", lb.name_en)
        assert card.official_website == lb.website
        assert card.contact is not None and card.contact.phone == "+977-61-521105"
        assert (card.district and card.district.code, card.province and card.province.code) == (
            KASKI,
            GANDAKI,
        )
        # Own links by position, then the district's.
        assert [link.title for link in card.quick_links] == [
            "Budget",
            "Ward Directives",
            "DAO Kaski",
        ]

    def test_district_and_province_cards(self, ref: ReferenceRepository) -> None:
        ref.add_region_link(GANDAKI, title_en="Province portal", url="https://gandaki.gov.np")
        district = RegionalCard.model_validate(ref.regional_card(KASKI))
        assert (district.level, district.contact, district.district) == ("district", None, None)
        assert district.province is not None and district.province.code == GANDAKI
        assert [link.title for link in district.quick_links] == ["Province portal"]

        province = RegionalCard.model_validate(ref.regional_card(GANDAKI))
        assert (province.level, province.province) == ("province", None)

    def test_unknown_code(self, ref: ReferenceRepository) -> None:
        assert ref.regional_card("MUN999999") is None


class TestRegionLinks:
    def test_re_adding_a_url_updates_it(self, ref: ReferenceRepository) -> None:
        first = ref.add_region_link(POKHARA, title_en="Old", url="https://x.gov.np/a")
        again = ref.add_region_link(POKHARA, title_en="New", url="https://x.gov.np/a", position=5)
        assert again.id == first.id
        assert (again.title_en, again.position, again.local_body_code) == ("New", 5, POKHARA)

    def test_remove_and_bad_input(self, ref: ReferenceRepository) -> None:
        link = ref.add_region_link(KASKI, title_en="Gone", url="https://x.gov.np/g")
        ref.remove_region_link(link.id)
        ref.session.flush()
        assert ref.session.get(RegionLink, link.id) is None
        with pytest.raises(LookupError):
            ref.remove_region_link(link.id)
        with pytest.raises(LookupError, match="code"):
            ref.add_region_link("NOPE", title_en="x", url="https://x.gov.np")
        with pytest.raises(ValueError):
            ref.add_region_link(KASKI, title_en=" ", url="https://x.gov.np")

    def test_database_rejects_non_http_urls(self, ref: ReferenceRepository) -> None:
        ref.session.add(RegionLink(district_code=KASKI, title_en="ftp", url="ftp://x.gov.np"))
        with pytest.raises(IntegrityError, match="ck_region_links_url_http"):
            ref.session.flush()

    def test_database_rejects_two_regions(self, ref: ReferenceRepository) -> None:
        ref.session.add(
            RegionLink(
                district_code=KASKI, local_body_code=POKHARA, title_en="two", url="https://x"
            )
        )
        with pytest.raises(IntegrityError, match="ck_region_links_exactly_one_region"):
            ref.session.flush()


class TestContactBackfill:
    def _site_page(
        self, ref: ReferenceRepository, n: int, *, emails: list[str], phones: list[str],
        address: str | None = None,
    ) -> None:
        bronze, silver = BronzeRepository(ref.session), SilverRepository(ref.session)
        domain_id = bronze.ensure_domain(SITE)
        url = f"https://{SITE}/{n}"
        doc: dict[str, Any] = {
            "url": url,
            "normalized_url": url,
            "host": SITE,
            "content_hash": f"{n:064x}",
            "fetched_at": (_ANCIENT + timedelta(days=n)).isoformat(),
            "contact_info": {"address": address},
        }
        doc_id = bronze.save_document(doc).id
        silver.save_page(
            {
                "searchable_text": f"page {n}",
                "content_hash": f"{n:064x}",
                "extracted_metadata": {"contact_info": {"emails": emails, "phones": phones}},
            },
            crawled_document_id=doc_id,
            domain_id=domain_id,
        )

    def test_the_most_common_footer_values_fill_only_empty_fields(
        self, ref: ReferenceRepository
    ) -> None:
        lb = pokhara(ref)
        lb.phone, lb.email, lb.address = None, "curated@pokhara.gov.np", None
        ref.session.flush()
        ref.session.execute(
            Domain.__table__.update().where(Domain.domain == SITE).values(local_body_id=None)
        )
        footer = {"emails": ["info@pokhara-test.gov.np"], "phones": ["061-521105"]}
        self._site_page(ref, 1, **footer, address="New Road, Pokhara")
        self._site_page(ref, 2, **footer, address="New Road, Pokhara")
        self._site_page(ref, 3, emails=["ward8@pokhara-test.gov.np"], phones=["061-999999"],
                        address="Ward 8 office")
        domain = ref.session.scalar(select(Domain).where(Domain.domain == SITE))
        assert domain is not None
        domain.local_body_id = lb.id
        ref.session.flush()

        filled = ref.fill_local_body_contacts()

        ref.session.flush()
        ref.session.refresh(lb)
        assert lb.phone == "061-521105"
        assert lb.address == "New Road, Pokhara"
        assert lb.email == "curated@pokhara.gov.np"  # never overwritten
        assert filled["phone"] >= 1 and filled["address"] >= 1

        # The first run filled everything it could, so a second run finds nothing to do.
        assert ref.fill_local_body_contacts() == {"phone": 0, "email": 0, "address": 0}
