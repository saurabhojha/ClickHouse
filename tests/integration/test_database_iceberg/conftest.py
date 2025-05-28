def pytest_generate_tests(metafunc):
    if "started_cluster" in metafunc.fixturenames:
        # Parametrize at the module level so all tests use the same cluster instance
        metafunc.parametrize(
            "started_cluster",
            ["docker_compose_iceberg_catalog.yml", "docker_compose_iceberg_apache_lakekeeper_catalog.yml"],
            indirect=True,
            scope="module"
        )
