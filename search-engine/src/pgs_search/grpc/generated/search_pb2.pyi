from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SearchRequest(_message.Message):
    __slots__ = ("query", "province_code", "district_code", "municipality_id", "ward_number", "content_type", "language", "page", "limit")
    QUERY_FIELD_NUMBER: _ClassVar[int]
    PROVINCE_CODE_FIELD_NUMBER: _ClassVar[int]
    DISTRICT_CODE_FIELD_NUMBER: _ClassVar[int]
    MUNICIPALITY_ID_FIELD_NUMBER: _ClassVar[int]
    WARD_NUMBER_FIELD_NUMBER: _ClassVar[int]
    CONTENT_TYPE_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    LIMIT_FIELD_NUMBER: _ClassVar[int]
    query: str
    province_code: str
    district_code: str
    municipality_id: str
    ward_number: int
    content_type: str
    language: str
    page: int
    limit: int
    def __init__(self, query: _Optional[str] = ..., province_code: _Optional[str] = ..., district_code: _Optional[str] = ..., municipality_id: _Optional[str] = ..., ward_number: _Optional[int] = ..., content_type: _Optional[str] = ..., language: _Optional[str] = ..., page: _Optional[int] = ..., limit: _Optional[int] = ...) -> None: ...

class SearchResponse(_message.Message):
    __slots__ = ("status_code", "total_hits", "execution_time_ms", "results", "query_language", "degraded")
    STATUS_CODE_FIELD_NUMBER: _ClassVar[int]
    TOTAL_HITS_FIELD_NUMBER: _ClassVar[int]
    EXECUTION_TIME_MS_FIELD_NUMBER: _ClassVar[int]
    RESULTS_FIELD_NUMBER: _ClassVar[int]
    QUERY_LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_FIELD_NUMBER: _ClassVar[int]
    status_code: int
    total_hits: int
    execution_time_ms: int
    results: _containers.RepeatedCompositeFieldContainer[SearchResultItem]
    query_language: str
    degraded: bool
    def __init__(self, status_code: _Optional[int] = ..., total_hits: _Optional[int] = ..., execution_time_ms: _Optional[int] = ..., results: _Optional[_Iterable[_Union[SearchResultItem, _Mapping]]] = ..., query_language: _Optional[str] = ..., degraded: _Optional[bool] = ...) -> None: ...

class GeoRef(_message.Message):
    __slots__ = ("province_code", "province_name", "district_code", "district_name", "municipality_id", "municipality_name", "ward_number")
    PROVINCE_CODE_FIELD_NUMBER: _ClassVar[int]
    PROVINCE_NAME_FIELD_NUMBER: _ClassVar[int]
    DISTRICT_CODE_FIELD_NUMBER: _ClassVar[int]
    DISTRICT_NAME_FIELD_NUMBER: _ClassVar[int]
    MUNICIPALITY_ID_FIELD_NUMBER: _ClassVar[int]
    MUNICIPALITY_NAME_FIELD_NUMBER: _ClassVar[int]
    WARD_NUMBER_FIELD_NUMBER: _ClassVar[int]
    province_code: str
    province_name: str
    district_code: str
    district_name: str
    municipality_id: str
    municipality_name: str
    ward_number: int
    def __init__(self, province_code: _Optional[str] = ..., province_name: _Optional[str] = ..., district_code: _Optional[str] = ..., district_name: _Optional[str] = ..., municipality_id: _Optional[str] = ..., municipality_name: _Optional[str] = ..., ward_number: _Optional[int] = ...) -> None: ...

class SearchResultItem(_message.Message):
    __slots__ = ("id", "result_type", "title", "url", "domain", "snippet", "download_url", "file_size_bytes", "relevance_score", "language", "published_at", "geo")
    ID_FIELD_NUMBER: _ClassVar[int]
    RESULT_TYPE_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    SNIPPET_FIELD_NUMBER: _ClassVar[int]
    DOWNLOAD_URL_FIELD_NUMBER: _ClassVar[int]
    FILE_SIZE_BYTES_FIELD_NUMBER: _ClassVar[int]
    RELEVANCE_SCORE_FIELD_NUMBER: _ClassVar[int]
    LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    PUBLISHED_AT_FIELD_NUMBER: _ClassVar[int]
    GEO_FIELD_NUMBER: _ClassVar[int]
    id: str
    result_type: str
    title: str
    url: str
    domain: str
    snippet: str
    download_url: str
    file_size_bytes: int
    relevance_score: float
    language: str
    published_at: str
    geo: GeoRef
    def __init__(self, id: _Optional[str] = ..., result_type: _Optional[str] = ..., title: _Optional[str] = ..., url: _Optional[str] = ..., domain: _Optional[str] = ..., snippet: _Optional[str] = ..., download_url: _Optional[str] = ..., file_size_bytes: _Optional[int] = ..., relevance_score: _Optional[float] = ..., language: _Optional[str] = ..., published_at: _Optional[str] = ..., geo: _Optional[_Union[GeoRef, _Mapping]] = ...) -> None: ...
