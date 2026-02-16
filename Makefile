.PHONY: build-abi test-abi-blackbox test-sqlite-provider-py

build-abi:
	cargo build -p sqlite-provider-abi

test-abi-blackbox: build-abi
	SQLITE3_SPI_LIB="$$(./scripts/locate_sqlite_abi_lib.sh debug)" \
	python3 tests/abi_blackbox/test_sqlite_abi.py -v

test-sqlite-provider-py: build-abi
	SQLITE3_SPI_LIB="$$(./scripts/locate_sqlite_abi_lib.sh debug)" \
	python3 -m pytest -q tests/sqlite_provider_py
