"""Create durable project-state tables."""

from alembic import op

from data_descriptor.flyover_v2.database import metadata

revision = "0001_project_state"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    metadata.create_all(op.get_bind())


def downgrade() -> None:
    metadata.drop_all(op.get_bind())
