"""Gestor de Conexión a Base de Datos PostgreSQL de Solo Lectura (Zero-Mutation).

Proporciona configuración declarativa desacoplada y context managers con
garantía estricta de solo lectura (read_only=True) para conectar Prospector
Interno con la base de datos empresarial legacy y de producción.
"""

from contextlib import contextmanager
from dataclasses import dataclass
import os
from pathlib import Path
from typing import Generator

import dotenv
import psycopg
from psycopg.rows import dict_row

from prospector_interno.domain.exceptions import LegacySeederError


@dataclass(frozen=True)
class DatabaseConfig:
    """Configuración inmutable de conexión a PostgreSQL."""

    host: str = "localhost"
    port: int = 5434
    database: str = "platform_db"
    user: str = "postgres"
    password: str = "postgres"
    connect_timeout: int = 5
    read_only: bool = True

    @property
    def conn_info(self) -> str:
        """Cadena de conexión estándar para el driver psycopg."""
        opts = "options='-c default_transaction_read_only=on'" if self.read_only else ""
        base = (
            f"host={self.host} port={self.port} dbname={self.database} "
            f"user={self.user} password={self.password} "
            f"connect_timeout={self.connect_timeout}"
        )
        if opts:
            return f"{base} {opts}"
        return base


def get_database_config(
    env_path: Path | str | None = None,
    force_prod: bool = False,
) -> DatabaseConfig:
    """Carga la configuración de base de datos desde .env o variables de entorno.

    Si se ejecuta dentro del ciclo de pruebas unitarias (PYTEST_CURRENT_TEST)
    y no se fuerza producción (force_prod=False), prioriza la instancia local
    de pruebas para preservar el aislamiento de suites.

    Args:
        env_path: Ruta opcional a un archivo .env específico.
        force_prod: Si es True, fuerza la lectura de .env ignorando modo test.

    Returns:
        Instancia de DatabaseConfig tipada y validada.
    """
    is_pytest = "PYTEST_CURRENT_TEST" in os.environ
    if is_pytest and not force_prod and "FORCE_PROD_DB" not in os.environ:
        return DatabaseConfig(
            host=os.getenv("TEST_DB_HOST", "localhost"),
            port=int(os.getenv("TEST_DB_PORT", "5434")),
            database=os.getenv("TEST_DB_NAME", "platform_db"),
            user=os.getenv("TEST_DB_USER", "postgres"),
            password=os.getenv("TEST_DB_PASSWORD", "postgres"),
            connect_timeout=int(os.getenv("TEST_DB_TIMEOUT", "5")),
            read_only=True,
        )

    # Buscar archivo .env
    target_env = None
    if env_path is not None:
        p = Path(env_path)
        if p.exists():
            target_env = p
    else:
        candidates = [
            Path(".env"),
            Path("prospector_interno/.env"),
            Path(__file__).resolve().parents[3] / ".env",
            Path(__file__).resolve().parents[4] / ".env",
        ]
        for c in candidates:
            if c.exists() and c.is_file():
                target_env = c
                break

    env_values: dict[str, str | None] = {}
    if target_env is not None:
        parsed = dotenv.dotenv_values(target_env)
        env_values = {k: v for k, v in parsed.items() if v is not None}

    def _val(key_aliases: list[str], default: str) -> str:
        for k in key_aliases:
            if k in os.environ and os.environ[k]:
                return os.environ[k]
            if k in env_values and env_values[k]:
                return str(env_values[k])
        return default

    host = _val(["ip", "DB_HOST", "POSTGRES_HOST"], "localhost")
    port_str = _val(["port", "DB_PORT", "POSTGRES_PORT"], "5434")
    db_name = _val(["database", "DB_NAME", "POSTGRES_DB"], "platform_db")
    user = _val(["user", "DB_USER", "POSTGRES_USER"], "postgres")
    password = _val(["password", "DB_PASSWORD", "POSTGRES_PASSWORD"], "postgres")

    try:
        port = int(port_str)
    except ValueError:
        port = 5434

    return DatabaseConfig(
        host=host,
        port=port,
        database=db_name,
        user=user,
        password=password,
        connect_timeout=5,
        read_only=True,
    )


_HOST_REACHABLE_CACHE: dict[tuple[str, int], tuple[float, bool]] = {}


def _is_tcp_reachable(host: str, port: int, timeout: float = 2.0) -> bool:
    import socket
    import time

    if host in ("127.0.0.1", "localhost"):
        return True
    now = time.monotonic()
    cached = _HOST_REACHABLE_CACHE.get((host, port))
    if cached and (now - cached[0]) < 30.0:
        return cached[1]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    try:
        sock.connect((host, port))
        ok = True
    except OSError:
        ok = False
    finally:
        sock.close()
    _HOST_REACHABLE_CACHE[(host, port)] = (now, ok)
    return ok


def get_replica_config(read_only: bool = False) -> DatabaseConfig:
    """Retorna la configuración hacia PostgreSQL de DATAX (producción o réplica)."""
    is_pytest = "PYTEST_CURRENT_TEST" in os.environ and "FORCE_PROD_DB" not in os.environ
    if is_pytest:
        return DatabaseConfig(
            host=os.environ.get("PROSPECTOR_PG_HOST", "127.0.0.1"),
            port=int(os.environ.get("PROSPECTOR_PG_PORT", "5434")),
            database=os.environ.get("PROSPECTOR_PG_DB", "platform_db"),
            user=os.environ.get("PROSPECTOR_PG_USER", "postgres"),
            password=os.environ.get("PROSPECTOR_PG_PASSWORD", "postgres"),
            connect_timeout=int(os.environ.get("PROSPECTOR_PG_TIMEOUT", "2")),
            read_only=read_only,
        )

    env_cfg = get_database_config(force_prod=True)
    host = os.environ.get("PROSPECTOR_PG_HOST", env_cfg.host)
    port = int(os.environ.get("PROSPECTOR_PG_PORT", str(env_cfg.port)))
    database = os.environ.get("PROSPECTOR_PG_DB", env_cfg.database)
    user = os.environ.get("PROSPECTOR_PG_USER", env_cfg.user)
    password = os.environ.get("PROSPECTOR_PG_PASSWORD", env_cfg.password)
    timeout = int(os.environ.get("PROSPECTOR_PG_TIMEOUT", "5"))

    if "PROSPECTOR_PG_HOST" not in os.environ and not _is_tcp_reachable(host, port):
        return DatabaseConfig(
            host="127.0.0.1",
            port=5434,
            database="platform_db",
            user="postgres",
            password="postgres",
            connect_timeout=2,
            read_only=read_only,
        )

    return DatabaseConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password,
        connect_timeout=timeout,
        read_only=read_only,
    )


def _connect_kwargs(cfg: DatabaseConfig) -> dict[str, object]:
    """Construye argumentos seguros para psycopg.connect (tolerante a símbolos en passwords)."""
    kwargs: dict[str, object] = {
        "host": cfg.host,
        "port": cfg.port,
        "dbname": cfg.database,
        "user": cfg.user,
        "password": cfg.password,
        "connect_timeout": cfg.connect_timeout,
    }
    if cfg.read_only:
        kwargs["options"] = "-c default_transaction_read_only=on"
    return kwargs


@contextmanager
def get_readonly_connection(
    config: DatabaseConfig | None = None,
) -> Generator[psycopg.Connection, None, None]:
    """Context manager para conexiones a PostgreSQL estrictamente de solo lectura.

    Aplica bloqueo bidireccional contra mutaciones:
    1. conn.read_only = True a nivel de sesión de psycopg.
    2. options='-c default_transaction_read_only=on' a nivel de conexión del motor.

    Args:
        config: Configuración de conexión opcional. Si es None, se obtiene
            la configuración de réplica local (o .env si FORCE_PROD_DB=1).

    Yields:
        Conexión activa de psycopg configurada con row_factory=dict_row.

    Raises:
        LegacySeederError: Si no es posible conectar al servidor de base de datos.
    """
    if config is not None:
        cfg = config
    elif os.environ.get("FORCE_PROD_DB") == "1":
        cfg = get_database_config(force_prod=True)
    else:
        cfg = get_replica_config(read_only=True)

    try:
        with psycopg.connect(**_connect_kwargs(cfg), row_factory=dict_row) as conn:
            conn.read_only = True
            yield conn
    except psycopg.Error as err:
        raise LegacySeederError(
            f"Fallo de conexión de solo lectura a PostgreSQL "
            f"({cfg.host}:{cfg.port}): {err}"
        ) from err


@contextmanager
def get_readwrite_connection(
    config: DatabaseConfig | None = None,
) -> Generator[psycopg.Connection, None, None]:
    """Context manager de lectura/escritura hacia la réplica PostgreSQL operativa."""
    cfg = config or get_replica_config(read_only=False)
    if cfg.read_only:
        cfg = DatabaseConfig(
            host=cfg.host,
            port=cfg.port,
            database=cfg.database,
            user=cfg.user,
            password=cfg.password,
            connect_timeout=cfg.connect_timeout,
            read_only=False,
        )
    try:
        with psycopg.connect(**_connect_kwargs(cfg), row_factory=dict_row) as conn:
            yield conn
    except psycopg.Error as err:
        raise LegacySeederError(
            f"Fallo de conexión operativa a PostgreSQL "
            f"({cfg.host}:{cfg.port}): {err}"
        ) from err
