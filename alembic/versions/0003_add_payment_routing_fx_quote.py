"""Add payment_route and fx_quote tables for multi-asset payment routing.

Revision ID: 0003
Revises:     0002
Create Date: 2026-08-27 00:00:00.000000 UTC

Creates the ``payment_route`` table to store execution route parameters
for Stellar asset -> fiat payout routing, and the ``fx_quote`` table to
store FX conversion quotes verified against live feeds before locking.
"""

from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# ---------------------------------------------------------------------------
# Revision identifiers
# ---------------------------------------------------------------------------
revision: str = "0003"
down_revision: Union[str, Sequence[str], None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# ---------------------------------------------------------------------------
# upgrade
# ---------------------------------------------------------------------------

def upgrade() -> None:
    """Create ``payment_route`` and ``fx_quote`` tables."""

    op.create_table(
        "payment_route",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("sender_currency", sa.String(16), nullable=False),
        sa.Column("receiver_currency", sa.String(16), nullable=False),
        sa.Column("source_asset", sa.String(128), nullable=False),
        sa.Column("target_rail", sa.String(64), nullable=False),
        sa.Column("provider", sa.String(128), nullable=False),
        sa.Column("rate", sa.Numeric(20, 10), nullable=False),
        sa.Column("fee", sa.Numeric(20, 10), nullable=False),
        sa.Column("estimated_amount", sa.Numeric(20, 10), nullable=False),
        sa.Column("slippage_bps", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("liquidity_pool_id", sa.String(128), nullable=True),
        sa.Column("status", sa.String(16), nullable=False, server_default="ACTIVE"),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_index(
        "ix_payment_route_currencies",
        "payment_route",
        ["sender_currency", "receiver_currency"],
    )
    op.create_index(
        "ix_payment_route_target_rail",
        "payment_route",
        ["target_rail"],
    )
    op.create_index(
        "ix_payment_route_status_priority",
        "payment_route",
        ["status", "priority"],
    )
    op.create_unique_constraint(
        "uq_payment_route_route",
        "payment_route",
        ["sender_currency", "receiver_currency", "provider", "target_rail"],
    )

    op.create_table(
        "fx_quote",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("payment_route_id", sa.String(36), sa.ForeignKey("payment_route.id"), nullable=False),
        sa.Column("sender_currency", sa.String(16), nullable=False),
        sa.Column("receiver_currency", sa.String(16), nullable=False),
        sa.Column("input_amount", sa.Numeric(20, 10), nullable=False),
        sa.Column("output_amount", sa.Numeric(20, 10), nullable=False),
        sa.Column("rate", sa.Numeric(20, 10), nullable=False),
        sa.Column("fee", sa.Numeric(20, 10), nullable=False),
        sa.Column("slippage_bps", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("live_feed_rate", sa.Numeric(20, 10), nullable=False),
        sa.Column("feed_source", sa.String(128), nullable=False),
        sa.Column("feed_timestamp", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("rate_deviation_bps", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("status", sa.String(16), nullable=False, server_default="PENDING"),
        sa.Column("expires_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("locked_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("executed_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.func.now()),
    )

    op.create_index(
        "ix_fx_quote_payment_route_id",
        "fx_quote",
        ["payment_route_id"],
    )
    op.create_index(
        "ix_fx_quote_currencies",
        "fx_quote",
        ["sender_currency", "receiver_currency"],
    )
    op.create_index(
        "ix_fx_quote_status",
        "fx_quote",
        ["status"],
    )
    op.create_index(
        "ix_fx_quote_expires_at",
        "fx_quote",
        ["expires_at"],
    )


# ---------------------------------------------------------------------------
# downgrade
# ---------------------------------------------------------------------------

def downgrade() -> None:
    """Drop ``fx_quote`` and ``payment_route`` tables."""

    op.drop_table("fx_quote")
    op.drop_table("payment_route")
