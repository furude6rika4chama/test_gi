"""Service to collect data."""
from datetime import datetime as dt, date
import gzip
import json
from io import StringIO
import requests

from jsonschema import validate
from tools import (
    CONTEXT_SCHEMA,
    REPORT_SCHEMA,
    generate_report_url,
    load_df_to_db,
)


def get_service_data(
        url: str,
        report_name: str,
        report_date: date,
        report_format: str
):
    """Get service data (report) from url."""
    report_data = []
    error_data = []

    report_url = generate_report_url(url, report_name, report_date, report_format)
    response = requests.get(report_url)

    file_response = StringIO(gzip.decompress(response.content).decode())

    for line in file_response:
        try:
            line_json = json.loads(line)
            validate(instance=line_json, schema=REPORT_SCHEMA)
            validate(instance=line_json['context'], schema=CONTEXT_SCHEMA)

            line_json['ts'] = dt.utcfromtimestamp(line_json['ts']).strftime('%Y-%m-%d %H:%M:%S')
            report_data.append(line_json)
        except Exception as err:
            error_dict = {
                'api_report': report_name,
                'api_date': report_date,
                'row_text': line,
                'error_text': str(err.__class__.__name__) + ': ' + str(err),
                'ins_ts': dt.now()
            }
            error_data.append(error_dict)
    return report_data, error_data


if __name__ == "__main__":
    URL = 'https://snap.datastream.center/techquest'
    REPORT_NAME = 'input'
    REPORT_DATE = date(2017, 2, 1)
    REPORT_FORMAT = 'json'

    REPORT_DATA, ERROR_DATA = get_service_data(URL, REPORT_NAME, REPORT_DATE, REPORT_FORMAT)

    ENGINE = 'dummy_conn'
    load_df_to_db(ENGINE, REPORT_DATA, 'report_input')
    load_df_to_db(ENGINE, ERROR_DATA, 'data_error')
