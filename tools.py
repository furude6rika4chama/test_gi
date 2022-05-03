"""Tools for service to collect data."""
from datetime import datetime as dt, date
from io import StringIO
import pandas as pd
import psycopg2

from sqlalchemy.engine.base import Engine


MIN_UNIX_TIMESTAMP = dt.timestamp(dt(2000, 1, 1, 0, 0, 0))

REPORT_SCHEMA = {
    'type': 'object',
    'properties': {
        'user': {'type': 'integer'},
        'ts': {'type': 'number', 'minimum': MIN_UNIX_TIMESTAMP, },
        'context': {'type': 'object'},
        'ip': {'type': 'string'},
    },
    "required": ['user', 'ts', 'context', 'ip']
}

CONTEXT_SCHEMA = {
    'type': 'object',
    'properties': {
        'hard': {'type': 'integer'},
        'soft': {'type': 'integer'},
        'level': {'type': 'integer'},
    }
}


def generate_report_url(
        url: str,
        report_name: str,
        report_date: date,
        report_format: str
):
    """Generate url from params."""
    return '{url}/{report}-{date}.{format}.gz'.format(
        url=url,
        report=report_name,
        date=report_date.strftime('%Y-%m-%d'),
        format=report_format
    )


def load_df_to_db(
        engine: Engine,
        list_to_load: list,
        table_name: str
):
    """"Convert list to df and load to db."""
    df_to_load = pd.DataFrame(list_to_load)

    buffer = StringIO()
    df_to_load.to_csv(buffer, index_label="id", header=False, sep="\t")
    buffer.seek(0)

    connection = engine.raw_connection()
    cursor = connection.cursor()

    try:
        cursor.copy_from(buffer, table_name, sep="\t")
        connection.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        connection.rollback()
        raise error
    finally:
        cursor.close()
        connection.close()
