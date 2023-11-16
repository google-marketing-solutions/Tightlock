"""add Retries table

Revision ID: e05c7c48fbf2
Revises: b63959034284
Create Date: 2023-10-06 15:45:25.688447

"""
from alembic import op
import sqlalchemy as sa
import sqlmodel
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'e05c7c48fbf2'
down_revision = 'b63959034284'
branch_labels = None
depends_on = None


def upgrade() -> None:
  op.create_table(
      'retries',\
      sa.Column('id', sa.Integer(), nullable=False),
      sa.Column('connection_id',
                sqlmodel.sql.sqltypes.AutoString(),
                nullable=False),
      sa.Column('uuid', sqlmodel.sql.sqltypes.AutoString(), nullable=False),
      sa.Column('destination_type', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
      sa.Column('destination_folder', sqlmodel.sql.sqltypes.AutoString(), nullable=True),
      sa.Column('destination_config', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
      sa.Column('next_run', sa.DateTime(timezone=True), nullable=True),
      sa.Column('retry_num', sa.Integer(), nullable=False),
      sa.Column('delete', sa.Boolean(), nullable=False),
      sa.Column('data', postgresql.JSONB(astext_type=sa.Text()), nullable=True),
      sa.PrimaryKeyConstraint('id'))
  op.create_unique_constraint(None, 'retries', ['uuid'])
  pass


def downgrade() -> None:
  op.drop_table('retries')
  op.drop_constraint(None, 'retries', type_='unique')
