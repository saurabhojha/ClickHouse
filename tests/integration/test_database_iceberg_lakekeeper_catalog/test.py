import json
import logging
import random
import requests
import time
import uuid
from datetime import datetime

import pyarrow as pa
import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DoubleType,
    NestedField,
    StringType,
)

from helpers.cluster import ClickHouseCluster
from helpers.config_cluster import minio_secret_key, minio_access_key
from helpers.test_tools import TSV, csv_compare

BASE_URL_LOCAL = "http://localhost:8181/catalog"
BASE_URL = "http://lakekeeper:8181/catalog"
CATALOG_NAME = "demo"
WAREHOUSE_NAME = "demo"

DEFAULT_CREATE_TABLE = "CREATE TABLE {}.`{}.{}`\n(\n    `id` Nullable(Float64),\n    `data` Nullable(String)\n)\nENGINE = Iceberg('http://minio:9000/warehouse-rest/data/', 'minio', '[HIDDEN]')\n"


def create_warehouse(minio_ip):
    """Create the demo warehouse using the Lakekeeper management API"""
    logging.info("Creating warehouse 'demo' via Lakekeeper management API...")
    
    # Use MinIO internal IP so PyIceberg can reach it from test environment
    minio_endpoint = f"http://{minio_ip}:9000"
    logging.debug(f"Using MinIO endpoint for warehouse: {minio_endpoint}")
    
    warehouse_data = {
        "warehouse-name": "demo",
        "project-id": "00000000-0000-0000-0000-000000000000",
        "storage-profile": {
            "type": "s3",
            "bucket": "warehouse-rest",
            "key-prefix": "",
            "assume-role-arn": None,
            "endpoint": minio_endpoint,
            "region": "local-01",
            "path-style-access": True,
            "flavor": "minio",
            "sts-enabled": True
        },
        "storage-credential": {
            "type": "s3",
            "credential-type": "access-key",
            "aws-access-key-id": "minio",
            "aws-secret-access-key": "ClickHouse_Minio_P@ssw0rd"
        }
    }
    
    logging.debug(f"Warehouse configuration: {json.dumps(warehouse_data, indent=2)}")
    
    try:
        response = requests.post(
            "http://localhost:8181/management/v1/warehouse",
            headers={"Content-Type": "application/json"},
            json=warehouse_data,
            timeout=30
        )
        
        logging.debug(f"Warehouse creation response: {response.status_code} - {response.text}")
        
        if response.status_code == 201:
            logging.info("✓ Warehouse 'demo' created successfully")
        elif response.status_code == 409:
            logging.info("✓ Warehouse 'demo' already exists")
        else:
            logging.error(f"Failed to create warehouse: {response.status_code} - {response.text}")
            response.raise_for_status()
            
    except requests.exceptions.RequestException as e:
        logging.error(f"Failed to connect to Lakekeeper management API: {e}")
        raise


def load_catalog_impl(started_cluster):
    minio_ip = started_cluster.get_instance_ip('minio')
    s3_endpoint = f"http://{minio_ip}:9000"
    
    logging.debug(f"Initializing RestCatalog with:")
    logging.debug(f"  - warehouse: {WAREHOUSE_NAME}")
    logging.debug(f"  - uri: {BASE_URL_LOCAL}")
    logging.debug(f"  - s3.endpoint: {s3_endpoint}")
    logging.debug(f"  - s3.access-key-id: {minio_access_key}")
    
    return RestCatalog(
        name="my_catalog",
        warehouse=WAREHOUSE_NAME,
        uri=BASE_URL_LOCAL,
        token="dummy",
        **{
            "s3.endpoint": s3_endpoint,
            "s3.access-key-id": minio_access_key,
            "s3.secret-access-key": minio_secret_key,
        },
    )


@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster = ClickHouseCluster(__file__)
        cluster.add_instance(
            "node1",
            main_configs=[],
            user_configs=[],
            stay_alive=True,
            with_iceberg_catalog=True,
            extra_parameters={
                "docker_compose_file_name": "docker_compose_iceberg_apache_lake_keeper_catalog.yml"
            },
        )

        logging.info("Starting cluster...")
        cluster.start()

        # Wait for services to be ready
        logging.info("Waiting 15 seconds for services to be ready...")
        time.sleep(15)
        
        # Log service endpoints for debugging
        minio_ip = cluster.get_instance_ip('minio')
        logging.debug(f"MinIO internal IP: {minio_ip}")
        logging.debug(f"MinIO external endpoint: localhost:9002")
        logging.debug(f"Lakekeeper endpoint: localhost:8181")
        
        # Create the warehouse using the management API
        create_warehouse(minio_ip)

        yield cluster

    finally:
        cluster.shutdown()


def test_pyiceberg_standalone(started_cluster):
    """Test PyIceberg operations with Lakekeeper catalog"""
    import pandas as pd

    node = started_cluster.instances["node1"]
    
    logging.info("Starting PyIceberg standalone test...")
    logging.debug(f"MinIO endpoint: {started_cluster.get_instance_ip('minio')}:9000")
    
    catalog = load_catalog_impl(started_cluster)
    logging.info("✓ Connected to Lakekeeper catalog")
    
    # Create a new namespace if it doesn't already exist
    test_namespace = ("test_standalone",)
    existing_namespaces = catalog.list_namespaces()
    logging.debug(f"Existing namespaces: {existing_namespaces}")
    
    if test_namespace not in existing_namespaces:
        catalog.create_namespace(test_namespace)
        logging.info(f"✓ Created namespace: {test_namespace}")
    else:
        logging.info(f"✓ Namespace already exists: {test_namespace}")
    
    # Create test table with simple schema
    test_table_name = "my_table"
    test_table_identifier = test_namespace + (test_table_name,)
    logging.debug(f"Table identifier: {test_table_identifier}")
    
    # Drop table if exists
    try:
        existing_tables = catalog.list_tables(namespace=test_namespace)
        logging.debug(f"Existing tables in namespace: {existing_tables}")
        
        if test_table_identifier in existing_tables:
            catalog.drop_table(test_table_identifier)
            logging.info(f"✓ Dropped existing table: {test_table_identifier}")
    except Exception as e:
        logging.debug(f"Table drop failed (may not exist): {e}")
    
    # Create simple schema for test
    simple_schema = Schema(
        NestedField(field_id=1, name="id", field_type=DoubleType(), required=False),
        NestedField(field_id=2, name="data", field_type=StringType(), required=False),
    )
    logging.debug(f"Table schema: {simple_schema}")
    
    # Create table
    table = catalog.create_table(
        test_table_identifier,
        schema=simple_schema,
        properties={"write.metadata.compression-codec": "none"},
    )
    logging.info(f"✓ Created table: {test_table_identifier}")
    logging.debug(f"Table metadata location: {table.metadata_location}")
    
    # Write data
    df = pd.DataFrame(
        {
            "id": [1.0, 2.0, 3.0, 4.0, 5.0],  # Use float values to match DoubleType schema
            "data": ["hello", "world", "from", "lakekeeper", "test"],
        }
    )
    logging.debug(f"DataFrame to insert:\n{df}")
    
    pa_df = pa.Table.from_pandas(df)
    logging.debug(f"PyArrow table schema: {pa_df.schema}")
    
    table.append(pa_df)
    logging.info(f"✓ Inserted {len(df)} rows into table")
    
    # Read data back to verify PyIceberg works
    logging.debug("Starting table scan...")
    scan_result = table.scan().to_pandas()
    logging.info(f"✓ Read back {len(scan_result)} rows from table")
    logging.info(f"Data preview:\n{scan_result}")
    
    # Verify data
    logging.debug("Verifying data integrity...")
    assert len(scan_result) == 5
    assert list(scan_result["id"]) == [1.0, 2.0, 3.0, 4.0, 5.0]  # PyArrow converts int to float
    assert list(scan_result["data"]) == ["hello", "world", "from", "lakekeeper", "test"]
    
    logging.info("✓ All assertions passed - PyIceberg integration working correctly!")
    
    # List all namespaces and tables to verify
    namespaces = catalog.list_namespaces()
    logging.info(f"✓ All namespaces: {namespaces}")
    
    tables = catalog.list_tables(namespace=test_namespace)
    logging.info(f"✓ Tables in {test_namespace}: {tables}")
    
    logging.info("✓ Complete PyIceberg + Lakekeeper test passed!")
    
    # Test ClickHouse integration with Lakekeeper catalog
    logging.info("🔄 Testing ClickHouse integration with Lakekeeper catalog...")

    create_clickhouse_iceberg_database(started_cluster, node, CATALOG_NAME)

    # Instead of SHOW CREATE TABLE assertion, check row count and content
    assert 5 == int(node.query("SELECT count(*) FROM demo.`test_standalone.my_table`"))

    result = node.query("SELECT id, data FROM demo.`test_standalone.my_table` ORDER BY id FORMAT TSV")
    expected = TSV("""
1   hello
2	world
3	from
4	lakekeeper
5	test
""")
    assert csv_compare(result, expected), f"got\n{result}\nwant\n{expected}"


def create_clickhouse_iceberg_database(
    started_cluster, node, name, additional_settings={}
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
CREATE DATABASE {name} ENGINE = DataLakeCatalog('{BASE_URL}', 'minio', '{minio_secret_key}')
SETTINGS {",".join((k+"="+repr(v) for k, v in settings.items()))}
    """
    )
    show_result = node.query(f"SHOW DATABASE {name}")
    assert minio_secret_key not in show_result
    assert "HIDDEN" in show_result
    
