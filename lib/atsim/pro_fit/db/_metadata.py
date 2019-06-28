import sqlalchemy as sa


def getMetadata():
    return _createMetaData()


def _createMetaData():
    metadata = sa.MetaData()

    # variables
    sa.Table(
        "variables",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "variable_name",
            sa.String,
            sa.ForeignKey("variable_keys.variable_name"),
            nullable=False,
        ),
        sa.Column(
            "candidate_id",
            sa.Integer,
            sa.ForeignKey("candidates.id"),
            nullable=False,
        ),
        sa.Column("value", sa.Float),
    )

    # variable_keys
    sa.Table(
        "variable_keys",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("variable_name", sa.String),
        sa.Column("fit_flag", sa.Boolean),
        sa.Column("low_bound", sa.Float, nullable=True),
        sa.Column("upper_bound", sa.Float, nullable=True),
        sa.Column("calculated_flag", sa.Boolean),
        sa.Column("calculation_expression", sa.String, nullable=True),
    )

    # candidates
    sa.Table(
        "candidates",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=True),
        sa.Column("iteration_number", sa.Integer),
        sa.Column("candidate_number", sa.Integer),
        sa.Column("merit_value", sa.Integer),
    )

    # jobas
    sa.Table(
        "jobs",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "candidate_id",
            sa.Integer,
            sa.ForeignKey("candidates.id"),
            nullable=False,
        ),
        sa.Column("job_name", sa.String),
    )

    # evaluators
    sa.Table(
        "evaluated",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "job_id", sa.Integer, sa.ForeignKey("jobs.id"), nullable=False
        ),
        sa.Column("evaluator_name", sa.String),
        sa.Column("value_name", sa.String),
        sa.Column("expected_value", sa.Float),
        sa.Column("extracted_value", sa.Float),
        sa.Column("weight", sa.Float),
        sa.Column("merit_value", sa.Float),
        sa.Column(
            "evaluatorerror_id",
            sa.Integer,
            sa.ForeignKey("evaluatorerror.id"),
            nullable=True,
        ),
    )

    # evaluatorerror
    sa.Table(
        "evaluatorerror",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("msg", sa.String),
    )

    # runstatus
    sa.Table(
        "runstatus",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("title", sa.String, nullable=True),
        sa.Column("runstatus", sa.Enum("Running", "Finished", "Error")),
    )

    return metadata
