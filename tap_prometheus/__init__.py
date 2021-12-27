#!/usr/bin/env python3

from datetime import datetime
import pytz
import os
import json

import singer
from singer import utils, Transformer
from singer import metadata

from promalyze import Client

REQUIRED_CONFIG_KEYS = ['endpoint', 'start_date', 'metrics']
STATE = {}

LOGGER = singer.get_logger()

DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'


class Context:
    config = {}
    state = {}
    catalog = {}
    tap_start = None
    stream_map = {}
    new_counts = {}
    updated_counts = {}

    @classmethod
    def get_catalog_entry(cls, stream_name):
        if not cls.stream_map:
            cls.stream_map = {s["tap_stream_id"]: s for s in cls.catalog['streams']}
        return cls.stream_map.get(stream_name)

    @classmethod
    def get_schema(cls, stream_name):
        stream = [s for s in cls.catalog["streams"]
                  if s["tap_stream_id"] == stream_name][0]
        return stream["schema"]

    @classmethod
    def is_selected(cls, stream_name):
        stream = cls.get_catalog_entry(stream_name)
        if stream is not None:
            stream_metadata = metadata.to_map(stream['metadata'])
            return metadata.get(stream_metadata, (), 'selected')
        return False

    @classmethod
    def print_counts(cls):
        LOGGER.info('------------------')
        for stream_name, stream_count in Context.new_counts.items():
            LOGGER.info('%s: %d new, %d updates',
                        stream_name,
                        stream_count,
                        Context.updated_counts[stream_name])
        LOGGER.info('------------------')


def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


# Load schemas from schemas folder
def load_schema():
    filename = "aggregated_metric_history.json"
    path = get_abs_path('schemas') + '/' + filename
    with open(path) as file:
        schema: dict = json.load(file)
        return schema


def discover():
    raw_schema = load_schema()
    streams = []

    for metric in Context.config['metrics']:
        # build a schema by merging the raw schema and the metric configuration
        schema = raw_schema.copy()
        schema['properties']['labels'] = metric['labels']

        # create and add catalog entry
        catalog_entry = {
            'stream': metric['name'],
            'tap_stream_id': metric['name'],
            'schema': schema,
            # TODO Events may have a different key property than this. Change
            # if it's appropriate.
            'key_properties': ['date', 'metric', 'aggregation', 'labels']
        }
        streams.append(catalog_entry)

    return {'streams': streams}


def sync(client):
    # Write all schemas and init count to 0
    for catalog_entry in Context.catalog['streams']:
        stream_name = catalog_entry["tap_stream_id"]
        singer.write_schema(
            stream_name, catalog_entry['schema'], catalog_entry['key_properties'])

        Context.new_counts[stream_name] = 0
        Context.updated_counts[stream_name] = 0

    for metric in Context.config['metrics']:
        name = metric['name']
        query = metric['query']
        aggregations = []
        if 'aggregations' in metric:
            aggregations = metric['aggregations']
        if 'aggregation' in metric:
            aggregations = [metric['aggregation']]
        period = metric['period']
        step = metric['step']

        LOGGER.info('Loading metric "%s" using query "%s", aggregation: %s, period: %s, metric step: %s',
                    name, query, aggregations, period, step)

        query_metric(client, name, query, aggregations, period, step)


def query_metric(client, name, query, aggregations, period, step):
    stream_name = name # the stream has the same name as the metric in the config
    catalog_entry = Context.get_catalog_entry(stream_name)
    stream_schema = catalog_entry['schema']

    bookmark = get_bookmark(name)

    bookmark_unixtime = int(datetime.strptime(
        bookmark, DATE_FORMAT).replace(tzinfo=pytz.UTC).timestamp())
    extraction_time = singer.utils.now()
    current_unixtime = int(extraction_time.timestamp())

    if period == 'day':
        period_seconds = 86400
    else:
        raise Exception("Period is not supported: " + period)

    iterator_unixtime = bookmark_unixtime
    with Transformer(singer.UNIX_SECONDS_INTEGER_DATETIME_PARSING) as transformer:
        while iterator_unixtime + period_seconds <= current_unixtime:
            ts_data = client.range_query(
                query,
                start=iterator_unixtime,
                end=iterator_unixtime + period_seconds,
                step=step
            )  # returns PrometheusData object

            for ts in ts_data.timeseries:
                dataframe = ts.as_pandas_dataframe()
                dataframe['values'] = dataframe['values'].astype(float)
                labels = ts.metadata

                for aggregation in aggregations:
                    aggregated_value = aggregate(aggregation, dataframe)

                    # print(" " + str(bookmark_unixtime) + " "+ str(aggregated_value))
                    data = {
                        "date": iterator_unixtime,
                        "metric": name,
                        "labels": labels,
                        "aggregation": aggregation,
                        "value": aggregated_value
                    }
                    rec = transformer.transform(data, stream_schema)

                    singer.write_record(
                        stream_name,
                        rec,
                        time_extracted=extraction_time
                    )

                    Context.new_counts[stream_name] += 1

            if len(ts_data.timeseries) == 0:
                LOGGER.warn(
                    'Request %s returned an empty result for the date %s', query, iterator_unixtime)

            singer.write_bookmark(
                Context.state,
                name,
                'start_date',
                datetime.utcfromtimestamp(
                    iterator_unixtime + period_seconds).strftime(DATE_FORMAT)
            )

            # write state after every 100 records
            if (Context.new_counts[stream_name] % 100) == 0:
                singer.write_state(Context.state)

            iterator_unixtime += period_seconds

    singer.write_state(Context.state)


def aggregate(aggregation, dataframe):
    if aggregation == 'max':
        aggregated_value = dataframe.max()['values']
    elif aggregation == 'min':
        aggregated_value = dataframe.min()['values']
    elif aggregation == 'avg':
        aggregated_value = dataframe.mean()['values']
    else:
        raise Exception("Aggregation method not implemented: " + aggregation)
    return aggregated_value


def get_bookmark(name):
    bookmark = singer.get_bookmark(Context.state, name, 'start_date')
    if bookmark is None:
        bookmark = Context.config['start_date']
    return bookmark


def init_prom_client():
    auth = None
    if Context.config['auth']:
        auth = (Context.config['auth']['username'], Context.config['auth']['password'])

    return Client(Context.config['endpoint'], auth)


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)
    Context.config = args.config

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        print(json.dumps(catalog, indent=2))

    else:
        Context.tap_start = utils.now()
        if args.catalog:
            Context.catalog = args.catalog.to_dict()
        else:
            Context.catalog = discover()

        Context.state = args.state

        client = init_prom_client()
        sync(client)


if __name__ == '__main__':
    main()
