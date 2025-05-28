def pytest_generate_tests(metafunc):
    if "catalog_config" in metafunc.fixturenames:
        metafunc.parametrize(
            "catalog_config",
            # TODO: add "nessie_rest" post fixing the issues
            ["nessie_rest"],
            indirect=True,
            scope="module"
        )
