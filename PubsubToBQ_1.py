import argparse
import ast
import datetime
import json
import logging
import sys

import apache_beam as beam
import apache_beam.transforms.window as window
import dask.dataframe as dd
import pandas as pd
from apache_beam.options.pipeline_options import PipelineOptions
from google.cloud import storage, bigquery


class JobOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_argument(
            "--project_id",
            type=str,
            help="project ID of GCP project",
            default=None
        )
        parser.add_argument(
            "--input_subscription",
            type=str,
            help="The Cloud Pub/Sub topic to read from.\n"
                 '"projects/<PROJECT_NAME>/subscriptions/<SUBSCRIPTION_NAME>".',
        )
        parser.add_argument(
            "--window_size",
            type=float,
            help="Error file's window size in number of minutes.",
        )
        parser.add_argument(
            "--output_path",
            type=str,
            help="GCS Path of the directory. Must end with /",
        )
        parser.add_argument(
            "--output_file_prefix",
            type=str,
            help="GCS output file prefix",
        )
        parser.add_argument(
            "--bigquery_dataset",
            type=str,
            help="Bigquery Dataset to write raw salesforce data",
        )
        parser.add_argument(
            "--bigquery_table",
            type=str,
            help="Bigquery Table to write raw salesforce data",
        )


class AddTimestamps(beam.DoFn):
    def process(self, element, publish_time=beam.DoFn.TimestampParam):
        logging.getLogger().setLevel(logging.INFO)
        """Processes each incoming windowed element by extracting the Pub/Sub
        message and its publish timestamp into a dictionary. `publish_time`
        defaults to the publish timestamp returned by the Pub/Sub server. It
        is bound to each element by Beam at runtime.
        """
        try:
            payload = json.loads(element.decode("utf-8"))
            # Data from Salesforce are coming into an array which might contain multiple changes.
            # We'll unwrap them and push them one by one
            for message in payload:
                message["publish_time"] = datetime.datetime.utcfromtimestamp(float(publish_time)).strftime("%Y-%m-%d %H:%M:%S.%f")
                yield message
        except (ValueError, AttributeError) as e:
            logging.info(f"[Invalid Data] ({e}) - {element}")
            pass


class GroupWindowsIntoBatches(beam.PTransform):
    """A composite transform that groups Pub/Sub messages based on publish
    time and outputs a list of dictionaries, where each contains one message
    and its publish timestamp.
    """

    def __init__(self, window_size):
        # Convert minutes into seconds.
        self.window_size = int(window_size * 60)

    def expand(self, pcoll):
        return (
            pcoll
            # Assigns window info to each Pub/Sub message based on its publish timestamp.
            | "Window into Fixed Intervals" >> beam.WindowInto(window.FixedWindows(self.window_size))
            # If the windowed elements do not fit into memory please consider using `beam.util.BatchElements`.
            | "Add Dummy Key" >> beam.Map(lambda elem: (None, elem))
            | "Groupby" >> beam.GroupByKey()
            | "Abandon Dummy Key" >> beam.MapTuple(lambda _, val: val)
        )


class WriteToGCS(beam.DoFn):
    def __init__(self, output_path, output_file_prefix):
        self.output_path = output_path
        self.output_file_prefix = output_file_prefix

    def start_bundle(self):
        self.client = storage.Client()

    def process(self, batch, window=beam.DoFn.WindowParam):
        # Load windows batch to a Dataframe
        df = pd.DataFrame.from_dict(data=batch)

        # De-Duplicate messages.
        df.sort_values(by=['publish_time'], inplace=True)

        # usually on chats we might get 2-3 updates at a very short time where only 1-2 fields change
        # we should only keep the last event since it's going to contain all the fields anyway
        df.drop_duplicates(subset="Id", keep='last', inplace=True)

        # Set GCS File path
        ts_format = "%FT%TZ"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        date_window_start = window.start.to_utc_datetime().strftime("%Y-%m-%d")
        filename = f"{self.output_file_prefix}-{window_start}-{window_end}"

        # Convert to Dask Dataframe
        ddf = dd.from_pandas(df, npartitions=1)
        ddf.to_json(f"{self.output_path}raw/dt={date_window_start}/{filename}-*.json.gz",
                    compression="gzip",
                    orient="records",
                    lines=True)

        yield df


class WriteDataframeToBQ(beam.DoFn):

    def __init__(self, bq_dataset, bq_table, project_id):
        self.bq_dataset = bq_dataset
        self.bq_table = bq_table
        self.project_id = project_id

    def start_bundle(self):
        self.client = bigquery.Client()

    def process(self, df):
        # table where we're going to store the data
        table_id = f"{self.bq_dataset}.{self.bq_table}"

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
            ],
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        try:
            load_job = self.client.load_table_from_dataframe(
                df,
                table_id,
                job_config=job_config,
            )  # Make an API request.

            load_job.result()  # Waits for the job to complete.
            if load_job.errors:
                logging.info(f"error_result =  {load_job.error_result}")
                logging.info(f"errors =  {load_job.errors}")
            else:
                logging.info(f'Loaded {len(df)} rows.')

        except Exception as error:
            logging.info(f'Error: {error} with loading dataframe')

            if load_job.errors:
                logging.info(f"error_result =  {load_job.error_result}")
                logging.info(f"errors =  {load_job.errors}")


def run(argv):
    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True, streaming=True)
    options = pipeline_options.view_as(JobOptions)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read PubSub Messages" >> beam.io.ReadFromPubSub(subscription=options.input_subscription)
            | "Add timestamps to messages" >> beam.ParDo(AddTimestamps())
            | f"Window into: {options.window_size}m" >> GroupWindowsIntoBatches(options.window_size)
            | "Export Raw Data to GCS" >> beam.ParDo(WriteToGCS(output_path=options.output_path, output_file_prefix=options.output_file_prefix))
            | "Write Raw Data to Big Query" >> beam.ParDo(WriteDataframeToBQ(project_id=options.project_id, bq_dataset=options.bigquery_dataset, bq_table=options.bigquery_table))
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
