import os
import time

import pytest
import random

from helpers.cluster import ClickHouseCluster

# Tests that sizes of in-memory caches (mark / uncompressed / index mark / index uncompressed / mmapped file / query cache) can be changed
# at runtime (issue #51085). This file tests only the mark cache (which uses the SLRU cache policy) and the query cache (which uses the TTL
# cache policy). As such, both tests are representative for the other caches.

cluster = ClickHouseCluster(__file__)
node = cluster.add_instance(
    "node",
    main_configs=["configs/default.xml"],
    stay_alive=True,
)

@pytest.fixture(scope="module")
def start_cluster():
    try:
        cluster.start()
        yield cluster
    finally:
        cluster.shutdown()


SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(SCRIPT_DIR, "configs")

def check_profile_metric_after_eviction(node, metric_name):
    node.query("SYSTEM FLUSH LOGS")
    value = node.query(f"""
            SELECT ProfileEvents['{metric_name}']
            FROM system.query_log
            WHERE type = 'QueryFinish'
              AND ProfileEvents['{metric_name}'] > 0
            ORDER BY query_start_time_microseconds DESC
            LIMIT 1
        """).strip()
    assert value > 0, f"{metric_name} was not triggered (value = {value})"


def test_mark_cache_eviction_metrics(start_cluster):
    node = cluster.instances["node"]

    # Create a table with enough parts and data to overflow the mark cache
    node.query("DROP TABLE IF EXISTS cache_test")
    node.query("""
        CREATE TABLE cache_test (id UInt64, val String)
        ENGINE = MergeTree()
        ORDER BY id
        SETTINGS index_granularity = 8192
    """)

    for i in range(20000):  # Insert many parts
        node.query(f"""
            INSERT INTO cache_test
            SELECT number, toString(number)
            FROM numbers(100000)
        """)
        time.sleep(0.2)  # Let merges flush if needed

    # Run queries to force mark loading and eviction
    for _ in range(100):
        start = random.randint(0, 90000)
    end = start + random.randint(5000, 15000)
    node.query(f"SELECT count() FROM cache_test WHERE id BETWEEN {start} AND {end}")

    # Flush and check metrics
    check_profile_metric_after_eviction(node, "MarkCacheEvictedBytes")
    check_profile_metric_after_eviction(node, "MarkCacheEvictedMarks")
    check_profile_metric_after_eviction(node, "MarkCacheEvictedFiles")

