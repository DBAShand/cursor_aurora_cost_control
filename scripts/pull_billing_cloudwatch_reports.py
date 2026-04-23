#!/usr/bin/env python3
"""Collect CloudWatch billing metrics for reporting.

This script is intended to run with a Billing-focused IAM role/profile.
It reads AWS/Billing EstimatedCharges metrics from CloudWatch (us-east-1)
and writes both JSON and CSV output files that can be used for reporting.
"""

from __future__ import annotations

import argparse
import csv
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import boto3
from botocore.exceptions import ClientError, ProfileNotFound


BILLING_NAMESPACE = "AWS/Billing"
BILLING_METRIC_NAME = "EstimatedCharges"
BILLING_REGION = "us-east-1"


@dataclass
class MetricTarget:
    """Describes one billing metric query target."""

    label: str
    dimensions: List[Dict[str, str]]


def profile_not_found_message(profile_name: str) -> str:
    return (
        f"AWS profile '{profile_name}' was not found.\n"
        "Use a real configured AWS profile name, or omit --profile to use environment/default credentials.\n"
        "To see configured profiles, run: aws configure list-profiles"
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull billing/report metrics from CloudWatch."
    )
    parser.add_argument(
        "--profile",
        help="AWS named profile that has Billing permissions.",
    )
    parser.add_argument(
        "--role-arn",
        help="IAM role ARN to assume for Billing data access.",
    )
    parser.add_argument(
        "--external-id",
        help="Optional external ID when assuming role.",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24 * 30,
        help="Lookback window in hours (default: 720 = last 30 days).",
    )
    parser.add_argument(
        "--period-seconds",
        type=int,
        default=3600,
        help="CloudWatch period in seconds (default: 3600).",
    )
    parser.add_argument(
        "--output-dir",
        default="output/billing",
        help="Output directory for report files.",
    )
    parser.add_argument(
        "--currency",
        default="USD",
        help="Billing currency dimension value (default: USD).",
    )
    return parser.parse_args()


def build_session(args: argparse.Namespace) -> boto3.Session:
    if args.role_arn:
        try:
            base_session = (
                boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
            )
        except ProfileNotFound:
            print(profile_not_found_message(args.profile))
            raise SystemExit(2)
        sts = base_session.client("sts")
        assume_role_args = {
            "RoleArn": args.role_arn,
            "RoleSessionName": "billing-cloudwatch-report",
        }
        if args.external_id:
            assume_role_args["ExternalId"] = args.external_id
        credentials = sts.assume_role(**assume_role_args)["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=BILLING_REGION,
        )

    try:
        return boto3.Session(profile_name=args.profile, region_name=BILLING_REGION)
    except ProfileNotFound:
        print(profile_not_found_message(args.profile))
        raise SystemExit(2)


def discover_services(
    cloudwatch_client,
    currency: str,
) -> List[str]:
    """Discover ServiceName dimensions from AWS/Billing metrics."""
    services = set()
    paginator = cloudwatch_client.get_paginator("list_metrics")
    for page in paginator.paginate(
        Namespace=BILLING_NAMESPACE,
        MetricName=BILLING_METRIC_NAME,
        Dimensions=[{"Name": "Currency", "Value": currency}],
    ):
        for metric in page.get("Metrics", []):
            for dimension in metric.get("Dimensions", []):
                if dimension["Name"] == "ServiceName":
                    services.add(dimension["Value"])
    return sorted(services)


def chunked(items: Iterable[MetricTarget], size: int) -> Iterable[List[MetricTarget]]:
    """Yield fixed-size chunks to stay under CloudWatch query limits."""
    batch: List[MetricTarget] = []
    for item in items:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch


def query_estimated_charges(
    cloudwatch_client,
    targets: List[MetricTarget],
    start_time: datetime,
    end_time: datetime,
    period_seconds: int,
) -> Dict[str, List[Dict[str, object]]]:
    """Query EstimatedCharges for all targets using CloudWatch MetricData."""
    output: Dict[str, List[Dict[str, object]]] = {}

    for target_batch in chunked(targets, 100):
        metric_data_queries = []
        for index, target in enumerate(target_batch):
            query_id = f"m{index}"
            metric_data_queries.append(
                {
                    "Id": query_id,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": BILLING_NAMESPACE,
                            "MetricName": BILLING_METRIC_NAME,
                            "Dimensions": target.dimensions,
                        },
                        "Period": period_seconds,
                        "Stat": "Maximum",
                        "Unit": "None",
                    },
                    "Label": target.label,
                    "ReturnData": True,
                }
            )

        response = cloudwatch_client.get_metric_data(
            MetricDataQueries=metric_data_queries,
            StartTime=start_time,
            EndTime=end_time,
            ScanBy="TimestampAscending",
        )

        for result in response.get("MetricDataResults", []):
            datapoints = []
            timestamps = result.get("Timestamps", [])
            values = result.get("Values", [])
            for ts, value in sorted(zip(timestamps, values), key=lambda row: row[0]):
                datapoints.append(
                    {
                        "timestamp": ts.isoformat(),
                        "estimated_charge": value,
                    }
                )
            output[result.get("Label", "unknown")] = datapoints

    return output


def write_outputs(
    output_dir: Path,
    currency: str,
    service_metrics: Dict[str, List[Dict[str, object]]],
    window_start: datetime,
    window_end: datetime,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "billing_cloudwatch_report.json"
    csv_path = output_dir / "billing_cloudwatch_report.csv"

    payload = {
        "namespace": BILLING_NAMESPACE,
        "metric_name": BILLING_METRIC_NAME,
        "currency": currency,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "services": service_metrics,
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(
            csv_file,
            fieldnames=["service_name", "timestamp", "estimated_charge"],
        )
        writer.writeheader()
        for service_name, datapoints in service_metrics.items():
            for row in datapoints:
                writer.writerow(
                    {
                        "service_name": service_name,
                        "timestamp": row["timestamp"],
                        "estimated_charge": row["estimated_charge"],
                    }
                )

    print(f"Wrote JSON report: {json_path}")
    print(f"Wrote CSV report:  {csv_path}")


def main() -> int:
    args = parse_args()
    session = build_session(args)
    cloudwatch = session.client("cloudwatch", region_name=BILLING_REGION)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=args.hours)

    try:
        services = discover_services(cloudwatch, args.currency)
    except ClientError as error:
        print(f"Failed to list billing metrics: {error}")
        return 1

    if not services:
        print(
            "No billing metrics found. Confirm Billing data is enabled "
            "and credentials are scoped correctly."
        )
        return 1

    targets = [
        MetricTarget(
            label=service,
            dimensions=[
                {"Name": "Currency", "Value": args.currency},
                {"Name": "ServiceName", "Value": service},
            ],
        )
        for service in services
    ]

    # Include overall account-level charge by omitting ServiceName.
    targets.insert(
        0,
        MetricTarget(
            label="Total",
            dimensions=[{"Name": "Currency", "Value": args.currency}],
        ),
    )

    try:
        service_metrics = query_estimated_charges(
            cloudwatch,
            targets=targets,
            start_time=start_time,
            end_time=end_time,
            period_seconds=args.period_seconds,
        )
    except ClientError as error:
        print(f"Failed to query billing metric data: {error}")
        return 1

    write_outputs(
        output_dir=Path(args.output_dir),
        currency=args.currency,
        service_metrics=service_metrics,
        window_start=start_time,
        window_end=end_time,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
