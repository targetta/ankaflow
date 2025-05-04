# ruff: noqa


class Fn:
    """
    Miscellaneous utility and convenience macros (UDFs)
    """

    columns = """(a) AS TABLE
        FROM query(
        concat("SELECT column_name, data_type FROM
            information_schema.columns WHERE table_name='"
        ,a,
        "'"));
    """
    calendar = """() AS TABLE
        FROM generate_series(
        current_date - INTERVAL '4' YEAR
        ,current_date + INTERVAL '1' YEAR,
        INTERVAL '1' DAY)
        SELECT
            generate_series AS date
            ,strftime(generate_series, '%Y') AS year
            ,strftime(generate_series, '%b') AS month
            ,strftime(generate_series, '%m') AS month_no
            ,strftime(generate_series, '%d') AS day
            ,strftime(generate_series, '%a') AS weekday
            ,strftime(generate_series, '%u') AS weekday_no
            ,strftime(generate_series, '%G') AS isoyear
            ,strftime(generate_series, '%V') AS iso_week
            ,strftime(generate_series, '%G-%V-%u') AS isoweekdate
            ,strftime(generate_series, '%G-00-%V') AS isoweek
            ,strftime(generate_series, '%V-%u') AS iso_week_day
            ,strftime(generate_series, '%x') AS isodate
            ,quarter(generate_series) AS quarter
            ,yearweek(generate_series) AS yearweek
        ;
    """
    add = "(a, b) AS a + b"
    plus = "(a, b) AS a + b"
    minus = "(a, b) AS a - b"
    div = "(a, b) AS IFNULL(a / NULLIF(b, 0), 0)"
    mult = "(a, b) AS a * b"

    float = "(a) AS IFNULL(TRY_CAST(a AS FLOAT), 0.0)"
    int = "(a) AS IFNULL(TRY_CAST(a AS BIGINT), 0::BIGINT)"
    str = "(a) AS IFNULL(TRY_CAST(a AS VARCHAR), '')"
    dt = """
    (a, fail_on_error := FALSE) AS
    CASE
        -- Case 1: ISO string with optional timezone → strip and cast
        WHEN TRY_CAST(REGEXP_REPLACE(CAST(a AS TEXT), '(Z|[+-][0-9]{2}:[0-9]{2})$', '') AS TIMESTAMP) IS NOT NULL
            THEN CAST(REGEXP_REPLACE(CAST(a AS TEXT), '(Z|[+-][0-9]{2}:[0-9]{2})$', '') AS TIMESTAMP)

        -- Case 2: Standard timestamp string
        WHEN TRY_CAST(a AS TIMESTAMP) IS NOT NULL
            THEN CAST(a AS TIMESTAMP)

        -- Case 3: ISO-style date
        WHEN TRY_CAST(a AS DATE) IS NOT NULL
            THEN CAST(a AS TIMESTAMP)

        -- Case 4: Unix time in seconds (int or float)
        WHEN TRY_CAST(a AS DOUBLE) IS NOT NULL
            AND CAST(a AS TEXT) ~ '^[0-9]+(\\.[0-9]+)?$'
            AND TRY_CAST(CAST(a AS DOUBLE) AS BIGINT) BETWEEN 1000000000 AND 9999999999
        THEN make_timestamp(CAST(CAST(a AS DOUBLE) * 1000000 AS BIGINT))  -- seconds → microseconds


        -- Case 5: nanoseconds (length > 15)
        WHEN TRY_CAST(a AS BIGINT) IS NOT NULL
            AND CAST(a AS TEXT) ~ '^[0-9]+$'
            AND LENGTH(CAST(a AS TEXT)) > 15
            THEN make_timestamp(CAST(TRY_CAST(a AS BIGINT) / 1000 AS BIGINT))

        -- Case 6: milliseconds
        WHEN TRY_CAST(a AS BIGINT) IS NOT NULL
            AND CAST(a AS TEXT) ~ '^[0-9]+$'
            THEN make_timestamp(CAST(TRY_CAST(a AS BIGINT) * 1000 AS BIGINT))

        -- Case 7: Explicit fail for other strings
        WHEN TYPEOF(a) = 'VARCHAR' AND LENGTH(CAST(a AS TEXT)) > 1 AND fail_on_error = TRUE
            THEN CAST('Unsupported format - use Fn.dt(value, pattern)' AS TIMESTAMP)

        -- Fallback
        ELSE make_timestamp(0)
    END,
    (value, pattern) AS (
            SELECT * FROM query(
                concat(
                    'SELECT STRPTIME(''',

                    -- Strip TZ suffix from value
                    REGEXP_REPLACE(value, '(Z|[+-][0-9]{2}:[0-9]{2}|[A-Za-z/_]+)$', ''),

                    ''',''',

                    -- Auto-detect and convert human-readable patterns
                    CASE
                        WHEN POSITION('%' IN pattern) > 0 THEN
                            REGEXP_REPLACE(REGEXP_REPLACE(pattern, '%z', ''), '%Z', '')
                        ELSE
                            REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                                pattern,
                                'YYYY', '%Y'),
                                'MM', '%m'),
                                'DD', '%d'),
                                'HH', '%H'),
                                'mm', '%M'),
                                'ss', '%S')
                    END,

                    ''')'
                )
            )
        );
    """
    """
    dt macro provides robust datetime parsing and normalization.

    Overload (a):
        Accepts any input value and attempts to convert it to a TIMESTAMP.
        - Supports TIMESTAMP_NS, TIMESTAMP, DATE, and BIGINT (UNIX ms).
        - Automatically strips time zone suffixes (Z or ±HH:MM) from strings.
        - Fails with a descriptive error if input is unrecognized.

    Overload (value, pattern):
        Dynamically parses a string using STRPTIME.
        - Strips unsupported time zone suffixes (Z or ±HH:MM) from input.
        - Removes %z and %Z from format string, as DuckDB does not interpret timezones.
        - Uses query() to compile a literal SQL expression for parsing.

    Returns:
        A DuckDB TIMESTAMP or a hard failure on invalid input.
    """
    # dt_format = "(a, format) AS STRFTIME(a, format)"
    dt_isoformat = "(a) AS STRFTIME(a, '%c')"
    dt_yyyy_mm_dd = "(a) AS STRFTIME(a, '%x')"
    dt_YYYY = "(a) AS STRFTIME(a, '%Y')"
    dt_MM = "(a) AS STRFTIME(a, '%m')"
    dt_DD = "(a) AS STRFTIME(a, '%d')"
    dt_HH = "(a) AS STRFTIME(a, '%H')"
    dt_iso_year = "(a) AS STRFTIME(a, '%G')"
    dt_iso_week = "(a) AS STRFTIME(a, '%V')"
    dt_iso_day = "(a) AS STRFTIME(a, '%u')"
    dt_iso_week_day = "(a) AS STRFTIME(a, '%V-%u')"
    dt_iso_weekdate = "(a) AS STRFTIME(a, '%V-%u')"
    dt_dayname = "(a) AS STRFTIME(a, '%G-%V-%u')"
    dt_monthname = "(a) AS STRFTIME(a, '%b')"
    dt_quarter = "(datum) AS QUARTER(CAST(datum AS TIMESTAMP))"

    dt_add = """
        (datum, days) AS CAST(datum AS TIMESTAMP) + TO_DAYS(CAST(days AS INT))
        """
    dt_boy = (
        "(datum) AS CAST(DATE_TRUNC('year', CAST(datum AS TIMESTAMP)) AS TIMESTAMP)"
    )
    dt_bom = (
        "(datum) AS CAST(DATE_TRUNC('month', CAST(datum AS TIMESTAMP)) AS TIMESTAMP)"
    )
    dt_eom = "(datum) AS CAST(LAST_DAY(CAST(datum AS TIMESTAMP)) AS TIMESTAMP)"
    dt_monday = (
        "(datum) AS CAST(DATE_TRUNC('week', CAST(datum AS TIMESTAMP)) AS TIMESTAMP)"
    )

    eq = "(a, b) AS a = b"
    ne = "(a, b) AS a <> b"
    gt = "(a, b) AS a > b"
    lt = "(a, b) AS a < b"
    gte = "(a, b) AS a >= b"
    lte = "(a, b) AS a <= b"

    ifelse = "(a, b, c) AS CASE WHEN a THEN b ELSE c END"
    bool = """(a) AS
        CASE
        WHEN a IS NULL THEN FALSE
        WHEN TRY_CAST(a AS BOOLEAN) IS NOT NULL THEN CAST(a AS BOOLEAN)
        WHEN TRY_CAST(a AS DOUBLE) IS NOT NULL AND CAST(a AS DOUBLE) = 0 THEN FALSE
        WHEN CAST(a AS VARCHAR) = '' THEN FALSE
        ELSE TRUE
        END
    """
    when = "(a, b, c) AS Fn.ifelse(Fn.bool(a), b, c)"
    and_ = "(a, b) AS Fn.bool(a) AND Fn.bool(b)"
    or_ = "(a, b) AS Fn.bool(a) OR Fn.bool(b)"
    not_ = "(a) AS Fn.bool(NOT Fn.bool(a))"

    uniquelist = "(a) AS LIST_DISTINCT(list(a))"
    has = "(list, element) AS LIST_CONTAINS(list, element)"

    includes = "(text, pattern) AS REGEXP_MATCHES(text, pattern)"
    extract = "(text, pattern) AS REGEXP_EXTRACT(text, pattern)"
    trim = "(a) AS trim(a)"
    month445 = """(date) AS
        CASE 
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN  1 AND  4 THEN strftime('%G-01', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN  5 AND  8 THEN strftime('%G-02', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN  9 AND 13 THEN strftime('%G-03', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN 14 AND 17 THEN strftime('%G-04', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN 18 AND 21 THEN strftime('%G-05', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN 22 AND 26 THEN strftime('%G-06', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN 27 AND 30 THEN strftime('%G-07', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN 31 AND 34 THEN strftime('%G-08', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN 35 AND 39 THEN strftime('%G-09', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN 40 AND 43 THEN strftime('%G-10', date)
            WHEN CAST(strftime('%V', date) AS INTEGER) BETWEEN 44 AND 47 THEN strftime('%G-11', date)
            ELSE strftime('%G-12', date)
        END   
    """
    validate_regex = """
    (value, pattern, allow_null) AS (
        CASE
            WHEN value IS NULL AND allow_null THEN NULL
            WHEN value IS NULL AND NOT allow_null THEN CAST('Validation failed: NULL value not allowed' AS INT)
            WHEN regexp_matches(value, pattern) THEN value
            ELSE CAST('Validation failed: ' || coalesce(value, 'NULL') || ' does not match pattern ' || pattern AS INT)
        END
    )
    """

    validate_gt = """
    (value, threshold, allow_null) AS (
        CASE
            WHEN value IS NULL AND allow_null THEN NULL
            WHEN value IS NULL AND NOT allow_null THEN CAST('Validation failed: NULL value not allowed' AS INT)
            WHEN value > threshold THEN value
            ELSE CAST('Validation failed: ' || coalesce(value::TEXT, 'NULL') || ' <= ' || threshold AS INT)
        END
    )
    """

    validate_lt = """
    (value, threshold, allow_null) AS (
        CASE
            WHEN value IS NULL AND allow_null THEN NULL
            WHEN value IS NULL AND NOT allow_null THEN CAST('Validation failed: NULL value not allowed' AS INT)
            WHEN value < threshold THEN value
            ELSE CAST('Validation failed: ' || coalesce(value::TEXT, 'NULL') || ' >= ' || threshold AS INT)
        END
    )
    """

    validate_between = """
    (value, min_val, max_val, allow_null) AS (
        CASE
            WHEN value IS NULL AND allow_null THEN NULL
            WHEN value IS NULL AND NOT allow_null THEN CAST('Validation failed: NULL value not allowed' AS INT)
            WHEN value BETWEEN min_val AND max_val THEN value
            ELSE CAST('Validation failed: ' || coalesce(value::TEXT, 'NULL') || ' not in [' || min_val || ', ' || max_val || ']' AS INT)
        END
    )
    """

    validate_not_between = """
    (value, min_val, max_val, allow_null) AS (
        CASE
            WHEN value IS NULL AND allow_null THEN NULL
            WHEN value IS NULL AND NOT allow_null THEN CAST('Validation failed: NULL value not allowed' AS INT)
            WHEN value NOT BETWEEN min_val AND max_val THEN value
            ELSE CAST('Validation failed: ' || coalesce(value::TEXT, 'NULL') || ' is within disallowed range [' || min_val || ', ' || max_val || ']' AS INT)
        END
    )
    """
