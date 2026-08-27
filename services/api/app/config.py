"""Environment-driven configuration (CB_*), aligned with the Rust services'
defaults so `docker compose up` + three `cargo run`s + one `uvicorn` just work.
"""

import os
from dataclasses import dataclass, field


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class Settings:
    amqp_url: str = field(
        default_factory=lambda: _env("CB_AMQP_URL", "amqp://coldbore:coldbore@localhost:5672/")
    )
    pg_url: str = field(
        default_factory=lambda: _env(
            "CB_PG_URL", "postgresql://coldbore:coldbore@localhost:5433/coldbore"
        )
    )
    mgmt_url: str = field(default_factory=lambda: _env("CB_MGMT_URL", "http://localhost:15672"))
    mgmt_user: str = field(default_factory=lambda: _env("CB_MGMT_USER", "coldbore"))
    mgmt_password: str = field(default_factory=lambda: _env("CB_MGMT_PASS", "coldbore"))
    poll_interval_s: float = field(
        default_factory=lambda: float(_env("CB_POLL_INTERVAL_S", "1.0"))
    )


settings = Settings()
