"""Tests specific to the duckdb dialect."""

from typing import Callable

import pytest
from _pytest.logging import LogCaptureFixture


@pytest.mark.parametrize(
    "segment_reference,raw",
    [
        ("CreateTypeStatementSegment", "create  type foo as struct(i integer,j varchar)"),
        ("CreateTypeStatementSegment", "create  type foo as map(integer, varchar)"),
        ("InsertStatementSegment", "INSERT INTO tbl VALUES (1), (2), (3)"),
        (
            "CreateTableStatementSegment",
            "CREATE TABLE t1 (i INTEGER NOT NULL DEFAULT 0,decimalnr DOUBLE CHECK (decimalnr < 10),date DATE UNIQUE,time TIMESTAMP)",
        ),
        ("WildcardExcludeExpressionSegment", "exclude (a, b, c)"),
        ("WildcardExcludeExpressionSegment", "exclude (a, b, c, )"),
        ("WildcardReplaceExpressionSegment", "replace (a + 1 as a, b || 'c' as b)"),
        ("WildcardReplaceExpressionSegment", "replace (a + 1 as a, b || 'c' as b,)"),
        ("WildcardRenameExpressionSegment", "rename (a as apple, b as berry)"),
        ("WildcardRenameExpressionSegment", "rename (a as apple, b as berry,)"),
        ("WildcardPatternMatchingSegment", "LIKE 'col%'"),
        ("WildcardPatternMatchingSegment", "GLOB 'col*'"),
        ("WildcardPatternMatchingSegment", "SIMILAR TO 'col.'"),
        ("LambdaExpressionSegment", "x -> x + 1"),  # syntax deprecated in duckdb 1.3
        ("LambdaExpressionSegment", "lambda x: x + 1"),  # syntax introduced in duckdb 1.3
        ("ListComprehensionExpressionSegment", "[upper(x) FOR x IN strings IF len(x) > 0]"),
        ("OrderByClauseSegment", "ORDER BY ALL"),
        ("OrderByClauseSegment", "order by 1, b"),
        ("GroupByClauseSegment", "GROUP BY ALL"),
        ("GroupByClauseSegment", "group by 1, b"),
        (
            "QualifyClauseSegment",
            "QUALIFY row_number() OVER (PARTITION BY schema_name ORDER BY function_name) < 3",
        ),
        (
            "FromPivotExpressionSegment",
            """PIVOT (
    sum(population)
    FOR
        year IN (2000, 2010, 2020)
    GROUP BY country
)""",
        ),
        (
            "SimplifiedPivotExpressionSegment",
            """PIVOT cities
ON year
USING first(population)
        """,
        ),
        (
            "FromUnpivotExpressionSegment",
            """FROM monthly_sales UNPIVOT (
    sales
    FOR month IN (jan, feb, mar, apr, may, jun)
)""",
        ),
        (
            "SimplifiedUnpivotExpressionSegment",
            """UNPIVOT monthly_sales
ON jan, feb, mar, apr, may, jun
INTO
    NAME month
    VALUE sales""",
        ),
        (
            "CreateFunctionStatementSegment",
            "CREATE MACRO ifelse(a, b, c) AS CASE WHEN a THEN b ELSE c END",
        ),
        (
            "CopyStatementSegment",
            "COPY lineitem FROM 'lineitem.json' (FORMAT json, AUTO_DETECT true)",
        ),
        (
            "CopyStatementSegment",
            "COPY lineitem TO 'lineitem.csv' (FORMAT csv, DELIMITER '|', HEADER)",
        ),
        (
            "CopyStatementSegment",
            "COPY (SELECT l_orderkey, l_partkey FROM lineitem) TO 'lineitem.parquet' (COMPRESSION zstd)",
        ),
    ],
)
def test_dialect_duckdb_specific_segment_parses(
    segment_reference: str,
    raw: str,
    caplog: LogCaptureFixture,
    dialect_specific_segment_parses: Callable,
) -> None:
    """Test that specific segments parse as expected.

    NB: We're testing the PARSE function not the MATCH function
    although this will be a recursive parse and so the match
    function of SUBSECTIONS will be tested if present. The match
    function of the parent will not be tested.
    """
    dialect_specific_segment_parses("duckdb", segment_reference, raw, caplog)


@pytest.mark.parametrize(
    "segment_reference,raw",
    [
        # parameter names required
        ("CreateTypeStatementSegment", "create  type foo as struct(integer, varchar)"),
        # group/order by all can't be used with other segments
        ("SelectStatementSegment", "select 1 a, 2 b order by 1, all"),
        ("SelectStatementSegment", "select 1 a, 2 b group by all, 1"),
    ],
)
def test_dialect_duckdb_segment_not_match(
    segment_reference: str,
    raw: str,
    caplog: LogCaptureFixture,
    dialect_specific_segment_not_match: Callable,
) -> None:
    """This validates one or multiple statements against specified segment class.

    It even validates the number of parsed statements with the number of expected
    statements.
    """
    return dialect_specific_segment_not_match("duckdb", segment_reference, raw, caplog)
