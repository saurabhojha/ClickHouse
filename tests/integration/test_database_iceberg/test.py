import glob
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta
from dataclasses import dataclass

import pyarrow as pa
import pytest
import requests
import urllib3
from minio import Minio
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import SortField, SortOrder
from pyiceberg.transforms import DayTransform, IdentityTransform
from pyiceberg.types import (
    DoubleType,
    FloatType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
)

from helpers.cluster import ClickHouseCluster, ClickHouseInstance, is_arm
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.s3_tools import get_file_contents, list_s3_objects, prepare_s3_bucket
from helpers.test_tools import TSV, csv_compare
from helpers.config_cluster import minio_secret_key


@dataclass
class CatalogConfig:
    CATALOG_TYPE: str
    BASE_URL: str
    BASE_URL_LOCAL: str
    BASE_URL_LOCAL_RAW: str
    DOCKER_COMPOSE_FILE: str
    CATALOG_NAME: str


DEFAULT_SCHEMA = Schema(
    NestedField(
        field_id=1, name="datetime", field_type=TimestampType(), required=False
    ),
    NestedField(field_id=2, name="symbol", field_type=StringType(), required=False),
    NestedField(field_id=3, name="bid", field_type=DoubleType(), required=False),
    NestedField(field_id=4, name="ask", field_type=DoubleType(), required=False),
    NestedField(
        field_id=5,
        name="details",
        field_type=StructType(
            NestedField(
                field_id=4,
                name="created_by",
                field_type=StringType(),
                required=False,
            ),
        ),
        required=False,
    ),
)

DEFAULT_CREATE_TABLE = "CREATE TABLE {}.`{}.{}`\\n(\\n    `datetime` Nullable(DateTime64(6)),\\n    `symbol` Nullable(String),\\n    `bid` Nullable(Float64),\\n    `ask` Nullable(Float64),\\n    `details` Tuple(created_by Nullable(String))\\n)\\nENGINE = Iceberg(\\'http://minio:9000/warehouse-rest/data/\\', \\'minio\\', \\'[HIDDEN]\\')\n"

DEFAULT_PARTITION_SPEC = PartitionSpec(
    PartitionField(
        source_id=1, field_id=1000, transform=DayTransform(), name="datetime_day"
    )
)

DEFAULT_SORT_ORDER = SortOrder(SortField(source_id=2, transform=IdentityTransform()))

def list_namespaces(catalog_confg):
    response = requests.get(f"{catalog_confg.BASE_URL_LOCAL}/namespaces")
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to list namespaces: {response.status_code}")

@pytest.fixture(scope="module")
def catalog_config(request):
    catalog_type = request.param
    if catalog_type == "spec_rest":
        return CatalogConfig(
            CATALOG_TYPE = "spec_rest",
            BASE_URL = "http://rest:8181/v1",
            BASE_URL_LOCAL = "http://localhost:8182/v1",
            BASE_URL_LOCAL_RAW = "http://localhost:8182",
            DOCKER_COMPOSE_FILE =  "docker_compose_iceberg_rest_catalog.yml",
            CATALOG_NAME = "demo"
        )
    elif catalog_type == "nessie_rest":
        return CatalogConfig(
            CATALOG_TYPE = "nessie_rest",
            BASE_URL = "http://rest:19120/iceberg/v1",
            BASE_URL_LOCAL = "http://localhost:19120/iceberg/v1/main",
            BASE_URL_LOCAL_RAW = "http://localhost:19120/iceberg",
            DOCKER_COMPOSE_FILE = "docker_compose_iceberg_nessie_rest_catalog.yml",
            CATALOG_NAME = "demo"
        )
    elif catalog_type == "apache_lakekeeper_rest":
        return CatalogConfig(
            CATALOG_TYPE = "apache_lakekeeper_rest",
            BASE_URL = "http://rest:8181/catalog/v1",
            BASE_URL_LOCAL = "http://localhost:8181/catalog/v1",
            BASE_URL_LOCAL_RAW = "http://localhost:8181/catalog",
            DOCKER_COMPOSE_FILE = "docker_compose_iceberg_apache_lake_keeper_catalog.yml",
            CATALOG_NAME = "demo"
        )

    raise ValueError(f"Unsupported catalog type: {catalog_type}")

def load_catalog_impl(started_cluster,catalog_config):
    return load_catalog(
        catalog_config.CATALOG_NAME,
        **{
            "uri": catalog_config.BASE_URL_LOCAL_RAW,
            "type": "rest",
            "s3.endpoint": f"http://{started_cluster.get_instance_ip('minio')}:9000",
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


def create_table(
    catalog,
    namespace,
    table,
    schema=DEFAULT_SCHEMA,
    partition_spec=DEFAULT_PARTITION_SPEC,
    sort_order=DEFAULT_SORT_ORDER,
):
    res = catalog.create_table(
        identifier=f"{namespace}.{table}",
        schema=schema,
        location=f"s3://warehouse-rest/data",
        partition_spec=partition_spec,
        sort_order=sort_order,
    )
    return res


def generate_record():
    return {
        "datetime": datetime.now(),
        "symbol": str("kek"),
        "bid": round(random.uniform(100, 200), 2),
        "ask": round(random.uniform(200, 300), 2),
        "details": {"created_by": "Alice Smith"},
    }


def create_clickhouse_iceberg_database(
    started_cluster, node, name,base_url, additional_settings={}
):
    settings = {
        "catalog_type": "rest",
        "warehouse": "demo",
        "storage_endpoint": "http://minio:9000/warehouse-rest",
    }

    settings.update(additional_settings)

    node.query(
        f"""
DROP DATABASE IF EXISTS {name};
SET allow_experimental_database_iceberg=true;
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{base_url}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )
    show_result = node.query(f"SHOW DATABASE {name}")
    assert minio_secret_key not in show_result
    assert "HIDDEN" in show_result

# For nessie catalog namespaces must be recursively created (e.g. clickhouse_x.testA.A)
def create_namespace_recursively(catalog, namespace: str):
    parts = namespace.split(".")
    for i in range(1, len(parts) + 1):
        sub_ns = ".".join(parts[:i])
        try:
            catalog.create_namespace(sub_ns)
        except NamespaceAlreadyExistsError as e:
            logging.info(f"Exception while creating namespace: {str(e)}")
            pass

def create_catalog_namespace(catalog,catalog_config, namespace: str):
    if catalog_config.CATALOG_TYPE == "nessie_rest":
        return create_namespace_recursively(catalog, namespace)
    return catalog.create_namespace(namespace)

@pytest.fixture(scope="module")
def started_cluster(catalog_config):
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
            extra_parameters={"docker_compose_file_name":catalog_config.DOCKER_COMPOSE_FILE}
        )

        logging.info("Starting cluster...")
        cluster.start()

        # TODO: properly wait for container
        time.sleep(10)

        yield cluster

    finally:
        cluster.shutdown()


def test_list_tables(started_cluster,catalog_config):
    node = started_cluster.instances["node1"]

    root_namespace = f"clickhouse_{uuid.uuid4()}"
    namespace_1 = f"{root_namespace}.testA.A"
    namespace_2 = f"{root_namespace}.testB.B"
    namespace_1_tables = ["tableA", "tableB"]
    namespace_2_tables = ["tableC", "tableD"]

    catalog = load_catalog_impl(started_cluster,catalog_config)

    for namespace in [namespace_1, namespace_2]:
        create_catalog_namespace(catalog,catalog_config, namespace)

    found = False
    for namespace_list in list_namespaces(catalog_config)["namespaces"]:
        if root_namespace == namespace_list[0]:
            found = True
            break
    assert found

    found = False
    for namespace_list in catalog.list_namespaces():
        if root_namespace == namespace_list[0]:
            found = True
            break
    assert found

    for namespace in [namespace_1, namespace_2]:
        assert len(catalog.list_tables(namespace)) == 0

    create_clickhouse_iceberg_database(started_cluster, node, catalog_config.CATALOG_NAME,catalog_config.BASE_URL,{})

    tables_list = ""
    for table in namespace_1_tables:
        create_table(catalog, namespace_1, table)
        if len(tables_list) > 0:
            tables_list += "\n"
        tables_list += f"{namespace_1}.{table}"

    for table in namespace_2_tables:
        create_table(catalog, namespace_2, table)
        if len(tables_list) > 0:
            tables_list += "\n"
        tables_list += f"{namespace_2}.{table}"

    result = node.query(
        f"SELECT name FROM system.tables WHERE database = '{catalog_config.CATALOG_NAME}' and name ILIKE '{root_namespace}%' ORDER BY name"
    ).strip()
    assert (
        tables_list
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{catalog_config.CATALOG_NAME}' and name ILIKE '{root_namespace}%' ORDER BY name"
        ).strip()
    )
    node.restart_clickhouse()
    assert (
        tables_list
        == node.query(
            f"SELECT name FROM system.tables WHERE database = '{catalog_config.CATALOG_NAME}' and name ILIKE '{root_namespace}%' ORDER BY name"
        ).strip()
    )

    expected = DEFAULT_CREATE_TABLE.format(catalog_config.CATALOG_NAME, namespace_2, "tableC")
    assert expected == node.query(
        f"SHOW CREATE TABLE {catalog_config.CATALOG_NAME}.`{namespace_2}.tableC`"
    )


def test_many_namespaces(started_cluster,catalog_config):
    node = started_cluster.instances["node1"]
    root_namespace_1 = f"A_{uuid.uuid4()}"
    root_namespace_2 = f"B_{uuid.uuid4()}"
    namespaces = [
        f"{root_namespace_1}",
        f"{root_namespace_1}.B.C",
        f"{root_namespace_1}.B.C.D",
        f"{root_namespace_1}.B.C.D.E",
        f"{root_namespace_2}",
        f"{root_namespace_2}.C",
        f"{root_namespace_2}.CC",
    ]
    tables = ["A", "B", "C"]
    catalog = load_catalog_impl(started_cluster,catalog_config)

    for namespace in namespaces:
        create_catalog_namespace(catalog,catalog_config, namespace)
        for table in tables:
            create_table(catalog, namespace, table)

    create_clickhouse_iceberg_database(started_cluster, node, catalog_config.CATALOG_NAME,catalog_config.BASE_URL,{})

    for namespace in namespaces:
        for table in tables:
            table_name = f"{namespace}.{table}"
            assert int(
                node.query(
                    f"SELECT count() FROM system.tables WHERE database = '{catalog_config.CATALOG_NAME}' and name = '{table_name}'"
                )
            )


def test_select(started_cluster,catalog_config):
    node = started_cluster.instances["node1"]

    test_ref = f"test_list_tables_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespace = f"{root_namespace}.A.B.C"
    namespaces_to_create = [
        root_namespace,
        f"{root_namespace}.A",
        f"{root_namespace}.A.B",
        f"{root_namespace}.A.B.C",
    ]

    catalog = load_catalog_impl(started_cluster,catalog_config)

    for namespace in namespaces_to_create:
        create_catalog_namespace(catalog,catalog_config, namespace)
        assert len(catalog.list_tables(namespace)) == 0

    table = create_table(catalog, namespace, table_name)

    num_rows = 10
    data = [generate_record() for _ in range(num_rows)]
    df = pa.Table.from_pylist(data)
    table.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, catalog_config.CATALOG_NAME,catalog_config.BASE_URL,{})

    expected = DEFAULT_CREATE_TABLE.format(catalog_config.CATALOG_NAME, namespace, table_name)
    assert expected == node.query(
        f"SHOW CREATE TABLE {catalog_config.CATALOG_NAME}.`{namespace}.{table_name}`"
    )

    assert num_rows == int(
        node.query(f"SELECT count() FROM {catalog_config.CATALOG_NAME}.`{namespace}.{table_name}`")
    )


def test_hide_sensitive_info(started_cluster,catalog_config):
    node = started_cluster.instances["node1"]

    test_ref = f"test_hide_sensitive_info_{uuid.uuid4()}"
    table_name = f"{test_ref}_table"
    root_namespace = f"{test_ref}_namespace"

    namespace = f"{root_namespace}.A"
    catalog = load_catalog_impl(started_cluster,catalog_config)
    create_catalog_namespace(catalog,catalog_config, namespace)

    table = create_table(catalog, namespace, table_name)

    create_clickhouse_iceberg_database(
        started_cluster,
        node,
        catalog_config.CATALOG_NAME,
        catalog_config.BASE_URL,
        additional_settings={"catalog_credential": "SECRET_1"},
    )
    assert "SECRET_1" not in node.query(f"SHOW CREATE DATABASE {catalog_config.CATALOG_NAME}")

    create_clickhouse_iceberg_database(
        started_cluster,
        node,
        catalog_config.CATALOG_NAME,
        catalog_config.BASE_URL,
        additional_settings={"auth_header": "SECRET_2"},
    )
    assert "SECRET_2" not in node.query(f"SHOW CREATE DATABASE {catalog_config.CATALOG_NAME}")


def test_tables_with_same_location(started_cluster,catalog_config):
    node = started_cluster.instances["node1"]

    test_ref = f"test_tables_with_same_location_{uuid.uuid4()}"
    namespace = f"{test_ref}_namespace"
    catalog = load_catalog_impl(started_cluster,catalog_config)

    table_name = f"{test_ref}_table"
    table_name_2 = f"{test_ref}_table_2"

    create_catalog_namespace(catalog,catalog_config, namespace)
    table = create_table(catalog, namespace, table_name)
    table_2 = create_table(catalog, namespace, table_name_2)

    def record(key):
        return {
            "datetime": datetime.now(),
            "symbol": str(key),
            "bid": round(random.uniform(100, 200), 2),
            "ask": round(random.uniform(200, 300), 2),
            "details": {"created_by": "Alice Smith"},
        }

    data = [record('aaa') for _ in range(3)]
    df = pa.Table.from_pylist(data)
    table.append(df)

    data = [record('bbb') for _ in range(3)]
    df = pa.Table.from_pylist(data)
    table_2.append(df)

    create_clickhouse_iceberg_database(started_cluster, node, catalog_config.CATALOG_NAME,catalog_config.BASE_URL,{})

    assert 'aaa\naaa\naaa' == node.query(f"SELECT symbol FROM {catalog_config.CATALOG_NAME}.`{namespace}.{table_name}`").strip()
    assert 'bbb\nbbb\nbbb' == node.query(f"SELECT symbol FROM {catalog_config.CATALOG_NAME}.`{namespace}.{table_name_2}`").strip()
