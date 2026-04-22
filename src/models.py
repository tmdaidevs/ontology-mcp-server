"""Pydantic models for Fabric Ontology definition structures."""

from __future__ import annotations

from pydantic import BaseModel, Field


class EntityTypeProperty(BaseModel):
    id: str
    name: str
    valueType: str = "String"
    redefines: str | None = None
    baseTypeNamespaceType: str | None = None


class EntityType(BaseModel):
    id: str
    namespace: str = "usertypes"
    baseEntityTypeId: str | None = None
    name: str
    entityIdParts: list[str] = Field(default_factory=list)
    displayNamePropertyId: str | None = None
    namespaceType: str = "Custom"
    visibility: str = "Visible"
    properties: list[EntityTypeProperty] = Field(default_factory=list)
    timeseriesProperties: list[EntityTypeProperty] = Field(default_factory=list)


class PropertyBinding(BaseModel):
    sourceColumnName: str
    targetPropertyId: str


class LakehouseTableSource(BaseModel):
    sourceType: str = "LakehouseTable"
    workspaceId: str
    itemId: str
    sourceTableName: str
    sourceSchema: str | None = None


class EventhouseTableSource(BaseModel):
    sourceType: str = "KustoTable"
    workspaceId: str
    itemId: str
    clusterUri: str
    databaseName: str
    sourceTableName: str


class DataBindingConfiguration(BaseModel):
    dataBindingType: str  # "TimeSeries" or "NonTimeSeries"
    timestampColumnName: str | None = None
    propertyBindings: list[PropertyBinding] = Field(default_factory=list)
    sourceTableProperties: dict  # LakehouseTableSource or EventhouseTableSource as dict


class DataBinding(BaseModel):
    id: str
    dataBindingConfiguration: DataBindingConfiguration


class Document(BaseModel):
    displayText: str | None = None
    url: str


class Widget(BaseModel):
    id: str
    type: str  # lineChart, barChart, file, graph, liveMap
    title: str | None = None
    yAxisPropertyId: str | None = None


class OverviewSettings(BaseModel):
    type: str  # fixedTime, customTime
    interval: str
    aggregation: str
    fixedTimeRange: str | None = None
    timeRange: dict | None = None


class Overview(BaseModel):
    widgets: list[Widget] = Field(default_factory=list)
    settings: OverviewSettings | None = None


class ResourceLink(BaseModel):
    type: str = "PowerBIReport"
    workspaceId: str
    itemId: str


class ResourceLinks(BaseModel):
    resourceLinks: list[ResourceLink] = Field(default_factory=list)


class RelationshipEnd(BaseModel):
    entityTypeId: str


class RelationshipType(BaseModel):
    id: str
    namespace: str = "usertypes"
    name: str
    namespaceType: str = "Custom"
    source: RelationshipEnd
    target: RelationshipEnd


class Contextualization(BaseModel):
    id: str
    dataBindingTable: dict  # LakehouseTableSource as dict
    sourceKeyRefBindings: list[PropertyBinding] = Field(default_factory=list)
    targetKeyRefBindings: list[PropertyBinding] = Field(default_factory=list)
