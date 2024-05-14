import subprocess
import time
import zipfile
from tempfile import NamedTemporaryFile
from typing import List, Tuple

import requests
from dagster import (
    AssetExecutionContext,
    AssetSpec,
    Backoff,
    OpExecutionContext,
    RetryPolicy,
    asset,
    file_relative_path,
)
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_duckdb import DuckDBResource
from dagster_embedded_elt.sling import (
    SlingMode,
    build_sling_asset,
)

from . import constants
from .resources import CustomDagsterDbtTranslator, dbt_manifest_path

retry_policy = RetryPolicy(
    max_retries=3,
    delay=0.2,  # 200ms
    backoff=Backoff.EXPONENTIAL,
)


def download_and_extract_data(
    context: AssetExecutionContext, url: str
) -> Tuple[List[str], float]:
    with NamedTemporaryFile(suffix=".zip") as f:
        start_time = time.time()
        context.log.info("Downloading checklist data from {}".format(url))
        r = requests.get(url)
        context.log.info("Downloaded {} bytes".format(len(r.content)))
        f.write(r.content)
        f.seek(0)

        with zipfile.ZipFile(f.name, "r") as zip_ref:
            extracted_names = zip_ref.namelist()
            zip_ref.extractall(
                file_relative_path(__file__, "../data/raw/checklist_data")
            )
            end_time = time.time()
            context.log.info(
                "Extracted checklist data to {}".format(
                    file_relative_path(__file__, "../raw/checklist_data")
                )
            )

        return extracted_names, end_time - start_time


@asset(compute_kind="python", group_name="raw_data")
def checklist_2020(context: AssetExecutionContext):
    extracted_names, elapsed_times = download_and_extract_data(
        context, constants.CHECKLIST_2020
    )
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@asset(compute_kind="python", group_name="raw_data")
def checklist_2023(context: AssetExecutionContext):
    extracted_names, elapsed_times = download_and_extract_data(
        context, constants.CHECKLIST_2023
    )
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@asset(compute_kind="python", group_name="raw_data")
def site_description_data(context: AssetExecutionContext):
    extracted_names, elapsed_times = download_and_extract_data(
        context, constants.SITE_DESCRIPTION_DATA
    )
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@asset(compute_kind="python", group_name="raw_data")
def species_translation_data(context: AssetExecutionContext):
    extracted_names, elapsed_times = download_and_extract_data(
        context, constants.SPECIES_TRANSLATION_DATA
    )
    context.add_output_metadata(
        metadata={
            "names": extracted_names,
            "num_files": len(extracted_names),
            "elapsed_time": elapsed_times,
        },
    )


@asset(
    deps=[checklist_2020, checklist_2023],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def birds(context: AssetExecutionContext, duckdb: DuckDBResource):
    cl2020 = file_relative_path(__file__, constants.CL_2020_FPATH)
    cl2023 = file_relative_path(__file__, constants.CL_2023_FPATH)
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE birds_2020_tmp AS (
                    SELECT * FROM read_csv_auto('{cl2020}', sample_size=-1))
            """
        )
        conn.execute(
            f"CREATE OR REPLACE TABLE birds_2023_tmp AS (SELECT * FROM read_csv_auto('{cl2023}'))"
        )
        conn.execute(
            """ CREATE OR REPLACE TABLE birds as (
                 SELECT * from birds_2020_tmp
                 UNION ALL
                 SELECT * from birds_2023_tmp
                )
            """
        )
        nrows = conn.execute("SELECT COUNT(*) FROM birds").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'birds'"
        ).pl()

    context.log.info("Created birds table")
    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )


@asset(
    deps=[species_translation_data],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def species(context: AssetExecutionContext, duckdb: DuckDBResource):
    species = file_relative_path(__file__, constants.SPECIES_TRANSLATION_FPATH)
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE species AS (
                    SELECT * FROM read_csv_auto('{species}'))
            """
        )
        nrows = conn.execute("SELECT COUNT(*) FROM species").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'species'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created species table")


@asset(
    deps=[site_description_data],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def sites(context: AssetExecutionContext, duckdb: DuckDBResource):
    sites = file_relative_path(__file__, constants.SITE_DATA_FPATH)
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE sites AS (
                    SELECT * FROM read_csv_auto('{sites}'))
            """
        )
        nrows = conn.execute("SELECT COUNT(*) FROM sites").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'sites'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created sites table")


@asset(group_name="raw_data", compute_kind="steampipe")
def bird_toots_csv(context: AssetExecutionContext):
    result = subprocess.run(
        ["steampipe", "query", constants.STEAMPIPE_QUERY, "--output", "csv"],
        stdout=subprocess.PIPE,
    )
    output = result.stdout.decode().strip()
    toot_path = file_relative_path(__file__, "../data/raw/bird_toots.csv")
    with open(toot_path, "w") as file:
        file.write(output)

    context.log.info("Created bird_toots file")


@asset(
    deps=[bird_toots_csv],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def bird_toots(context: AssetExecutionContext, duckdb: DuckDBResource):
    fpath = file_relative_path(__file__, "../data/raw/bird_toots.csv")
    with duckdb.get_connection() as conn:
        conn.execute(
            f"""CREATE OR REPLACE TABLE bird_toots AS (
                    SELECT * FROM read_csv_auto('{fpath}'))
            """
        )
        nrows = conn.execute("SELECT COUNT(*) FROM bird_toots").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'bird_toots'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created bird_toots table")


tickets_raw = build_sling_asset(
    AssetSpec(
        key=["tickets_raw"],
        group_name="raw_data",
    ),
    source_stream="tickets",
    target_object="tickets_raw",
    mode=SlingMode.FULL_REFRESH,
)

events_raw = build_sling_asset(
    AssetSpec(key=["events_raw"], group_name="raw_data"),
    source_stream="events",
    target_object="events_raw",
    mode=SlingMode.FULL_REFRESH,
)


@asset(
    deps=[tickets_raw],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def tickets(context: AssetExecutionContext, duckdb: DuckDBResource):
    with duckdb.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE tickets AS (SELECT * FROM tickets_raw)")
        nrows = conn.execute("SELECT COUNT(*) FROM tickets").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'tickets'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created tickets table")


@asset(
    deps=[events_raw],
    compute_kind="duckdb",
    group_name="prepared",
    retry_policy=retry_policy,
)
def events(context: AssetExecutionContext, duckdb: DuckDBResource):
    fpath = file_relative_path(__file__, "../data/raw/events.csv")
    with duckdb.get_connection() as conn:
        conn.execute("CREATE OR REPLACE TABLE events AS (SELECT * FROM events_raw)")
        nrows = conn.execute("SELECT COUNT(*) FROM events ").fetchone()[0]  # type: ignore

        metadata = conn.execute(
            "select * from duckdb_tables() where table_name = 'events'"
        ).pl()

    context.add_output_metadata(
        metadata={
            "num_rows": nrows,
            "table_name": metadata["table_name"][0],
            "datbase_name": metadata["database_name"][0],
            "schema_name": metadata["schema_name"][0],
            "column_count": metadata["column_count"][0],
            "estimated_size": metadata["estimated_size"][0],
        }
    )

    context.log.info("Created events table")


@dbt_assets(
    manifest=dbt_manifest_path, dagster_dbt_translator=CustomDagsterDbtTranslator()
)
def dbt_birds(context: OpExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()