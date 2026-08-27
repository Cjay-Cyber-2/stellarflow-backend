"""${message}

Revision ID: ${up_revision}
Revises:     ${down_revision | comma,n}
Create Date: ${create_date}

StellarFlow Backend — Alembic migration script.
Every migration MUST implement both upgrade() and downgrade() so that the
rollback test suite in tests/test_alembic_migrations.py can verify a clean
round-trip for every schema version.
"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# ---------------------------------------------------------------------------
# Revision identifiers (auto-filled by Alembic)
# ---------------------------------------------------------------------------
revision: str = ${repr(up_revision)}
down_revision: Union[str, Sequence[str], None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def upgrade() -> None:
    """Apply forward (up) migration."""
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    """Revert this migration completely.

    Every table/column/index created in upgrade() MUST be dropped here so
    that the rollback test suite can verify a clean round-trip.
    """
    ${downgrades if downgrades else "pass"}
