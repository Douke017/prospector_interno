"""Pruebas unitarias e integración para conexión PostgreSQL de solo lectura."""

from pathlib import Path
import pytest
import psycopg

from prospector_interno.adapters.storage.db_connection import (
    DatabaseConfig,
    get_database_config,
    get_readonly_connection,
)
from prospector_interno.domain.exceptions import LegacySeederError


def test_database_config_immutability_and_defaults() -> None:

    """Verifica valores predeterminados y regla inmutable de solo lectura."""
    cfg = DatabaseConfig(
        host="127.0.0.1",
        port=5434,
        database="platform_db",
        user="test_user",
        password="test_password",
    )
    assert cfg.host == "127.0.0.1"
    assert cfg.port == 5434
    assert cfg.database == "platform_db"
    assert cfg.read_only is True
    assert "default_transaction_read_only=on" in cfg.conn_info


def test_get_database_config_test_mode(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verifica que en contexto de test devuelve la configuración de prueba."""
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "active_test")
    monkeypatch.delenv("FORCE_PROD_DB", raising=False)
    cfg = get_database_config()
    assert cfg.host == "localhost"
    assert cfg.port == 5434
    assert cfg.database == "platform_db"
    assert cfg.read_only is True


def test_get_database_config_from_custom_env(tmp_path: Path) -> None:
    """Verifica que se leen variables personalizadas desde un archivo .env."""
    env_file = tmp_path / ".env.test"
    env_file.write_text(
        "ip = 192.168.1.100\n"
        "port = 5439\n"
        "database = custom_db\n"
        "user = custom_user\n"
        "password = secret_pwd\n",
        encoding="utf-8",
    )

    cfg = get_database_config(env_path=env_file, force_prod=True)
    assert cfg.host == "192.168.1.100"
    assert cfg.port == 5439
    assert cfg.database == "custom_db"
    assert cfg.user == "custom_user"
    assert cfg.password == "secret_pwd"
    assert cfg.read_only is True


def test_readonly_connection_prohibits_ddl() -> None:
    """Garantía Zero-Mutation: CREATE TABLE es rechazado por PostgreSQL."""

    test_cfg = DatabaseConfig(
        host="localhost",
        port=5434,
        database="platform_db",
        user="postgres",
        password="postgres",
        read_only=True,
    )

    with get_readonly_connection(test_cfg) as conn:
        assert conn.read_only is True
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                cur.execute("CREATE TABLE test_zero_mutation_probe (id int);")


def test_readonly_connection_prohibits_dml() -> None:
    """Garantía estricta Zero-Mutation: INSERT debe ser rechazado por PostgreSQL."""
    test_cfg = DatabaseConfig(
        host="localhost",
        port=5434,
        database="platform_db",
        user="postgres",
        password="postgres",
        read_only=True,
    )

    with get_readonly_connection(test_cfg) as conn:
        with conn.cursor() as cur:
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                cur.execute(
                    "INSERT INTO source (name, short_name) "
                    "VALUES ('Probe', 'PRB');"
                )


def test_readonly_connection_invalid_credentials_raises_domain_error() -> None:
    """Verifica manejo limpio de excepciones ante falla de conectividad."""
    invalid_cfg = DatabaseConfig(
        host="127.0.0.1",
        port=5999,
        database="non_existent",
        user="fake",
        password="fake",
        connect_timeout=1,
    )
    with pytest.raises(LegacySeederError) as exc_info:
        with get_readonly_connection(invalid_cfg):
            pass

    assert "Fallo de conexión de solo lectura" in str(exc_info.value)


def test_production_read_only_live_inspection() -> None:
    """Verifica lectura en vivo de producción si .env existe."""

    env_file = Path("prospector_interno/.env")
    if not env_file.exists():
        env_file = Path(".env")
    if not env_file.exists():
        pytest.skip(".env no disponible para prueba de producción")

    prod_cfg = get_database_config(env_path=env_file, force_prod=True)
    if prod_cfg.host == "localhost":
        pytest.skip("Configuración no apunta a host externo de producción")

    try:
        with get_readonly_connection(prod_cfg) as conn:
            assert conn.read_only is True
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) as count FROM source;")
                row = cur.fetchone()
                assert row is not None
                assert row["count"] >= 20

                # Re-validar rechazo absoluto a cualquier intento de mutación
                with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                    cur.execute("CREATE TABLE probe_prod_prevent (x int);")
    except LegacySeederError:
        pytest.skip("Servidor de producción no alcanzable en este momento")
