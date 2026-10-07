"""Code formats of the gazetteer (pgs_db.schemas.geography), shared by the routes."""

PROVINCE_CODE = r"^P[1-7]$"
DISTRICT_CODE = r"^D(?:0[1-9]|[1-6][0-9]|7[0-7])$"
LOCAL_BODY_CODE = r"^MUN\d{3}$"
