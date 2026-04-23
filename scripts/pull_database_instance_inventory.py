#!/usr/bin/env python3
"""Collect database/instance inventory and health signals.

This script is intended to run with a DatabaseAdministrator IAM role/profile.
It gathers RDS instance and EC2 instance metadata, plus recent CloudWatch
metrics for quick reporting and export.
"""

from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError, ProfileNotFound


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Pull instance information with DatabaseAdministrator role."
    )
    parser.add_argument(
        "--profile",
        help=(
            "AWS named profile used directly, or as the source profile "
            "when --role-arn is provided."
        ),
    )
    parser.add_argument(
        "--role-arn",
        help="IAM role ARN to assume for database inventory access.",
    )
    parser.add_argument(
        "--external-id",
        help="Optional external ID when assuming role.",
    )
    parser.add_argument(
        "--region",
        default="us-east-1",
        help="AWS region to query (default: us-east-1).",
    )
    parser.add_argument(
        "--hours",
        type=int,
        default=24,
        help="Hours of CloudWatch metrics to fetch (default: 24).",
    )
    parser.add_argument(
        "--output-dir",
        default="output/database",
        help="Output directory for report files.",
    )
    return parser.parse_args()


def build_session(args: argparse.Namespace) -> boto3.Session:
    if args.role_arn:
        base_session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
        sts = base_session.client("sts", region_name=args.region)
        assume_role_args = {
            "RoleArn": args.role_arn,
            "RoleSessionName": "database-instance-inventory",
        }
        if args.external_id:
            assume_role_args["ExternalId"] = args.external_id
        credentials = sts.assume_role(**assume_role_args)["Credentials"]
        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
            region_name=args.region,
        )

    return boto3.Session(profile_name=args.profile, region_name=args.region)


def build_profile_error_message(args: argparse.Namespace, error: ProfileNotFound) -> str:
    available_profiles = boto3.session.Session().available_profiles
    profile_help = (
        ", ".join(sorted(available_profiles))
        if available_profiles
        else "none detected"
    )
    profile_name = args.profile or "<not provided>"
    return (
        f"AWS profile '{profile_name}' was not found ({error}).\n"
        "If you copied an example command, replace placeholder values with your real profile.\n"
        "Run 'aws configure list-profiles' to see configured profiles.\n"
        f"Detected profiles: {profile_help}"
    )


def fetch_rds_instances(rds_client) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    paginator = rds_client.get_paginator("describe_db_instances")
    for page in paginator.paginate():
        for db in page.get("DBInstances", []):
            records.append(
                {
                    "instance_type": "RDS",
                    "identifier": db.get("DBInstanceIdentifier"),
                    "engine": db.get("Engine"),
                    "engine_version": db.get("EngineVersion"),
                    "status": db.get("DBInstanceStatus"),
                    "class_or_type": db.get("DBInstanceClass"),
                    "availability_zone": db.get("AvailabilityZone"),
                    "endpoint": (
                        db.get("Endpoint", {}).get("Address")
                        if db.get("Endpoint")
                        else None
                    ),
                    "allocated_storage_gb": db.get("AllocatedStorage"),
                    "multi_az": db.get("MultiAZ"),
                    "storage_encrypted": db.get("StorageEncrypted"),
                    "arn": db.get("DBInstanceArn"),
                }
            )
    return records


def fetch_ec2_instances(ec2_client) -> List[Dict[str, object]]:
    records: List[Dict[str, object]] = []
    paginator = ec2_client.get_paginator("describe_instances")
    for page in paginator.paginate():
        for reservation in page.get("Reservations", []):
            for instance in reservation.get("Instances", []):
                tags = {tag["Key"]: tag["Value"] for tag in instance.get("Tags", [])}
                records.append(
                    {
                        "instance_type": "EC2",
                        "identifier": instance.get("InstanceId"),
                        "engine": None,
                        "engine_version": None,
                        "status": instance.get("State", {}).get("Name"),
                        "class_or_type": instance.get("InstanceType"),
                        "availability_zone": instance.get("Placement", {}).get("AvailabilityZone"),
                        "endpoint": instance.get("PrivateIpAddress"),
                        "allocated_storage_gb": None,
                        "multi_az": None,
                        "storage_encrypted": None,
                        "arn": instance.get("InstanceArn"),
                        "name_tag": tags.get("Name"),
                    }
                )
    return records


def get_single_metric(
    cloudwatch_client,
    namespace: str,
    metric_name: str,
    dimensions: List[Dict[str, str]],
    start_time: datetime,
    end_time: datetime,
) -> Optional[float]:
    response = cloudwatch_client.get_metric_statistics(
        Namespace=namespace,
        MetricName=metric_name,
        Dimensions=dimensions,
        StartTime=start_time,
        EndTime=end_time,
        Period=3600,
        Statistics=["Average"],
    )
    datapoints = response.get("Datapoints", [])
    if not datapoints:
        return None
    latest = max(datapoints, key=lambda row: row["Timestamp"])
    return latest.get("Average")


def fetch_rds_metrics(
    cloudwatch_client,
    rds_instances: List[Dict[str, object]],
    start_time: datetime,
    end_time: datetime,
) -> Dict[str, Dict[str, Optional[float]]]:
    metrics: Dict[str, Dict[str, Optional[float]]] = {}
    for row in rds_instances:
        db_id = row["identifier"]
        dimensions = [{"Name": "DBInstanceIdentifier", "Value": db_id}]
        metrics[db_id] = {
            "cpu_average": get_single_metric(
                cloudwatch_client,
                namespace="AWS/RDS",
                metric_name="CPUUtilization",
                dimensions=dimensions,
                start_time=start_time,
                end_time=end_time,
            ),
            "free_storage_space_average_bytes": get_single_metric(
                cloudwatch_client,
                namespace="AWS/RDS",
                metric_name="FreeStorageSpace",
                dimensions=dimensions,
                start_time=start_time,
                end_time=end_time,
            ),
            "database_connections_average": get_single_metric(
                cloudwatch_client,
                namespace="AWS/RDS",
                metric_name="DatabaseConnections",
                dimensions=dimensions,
                start_time=start_time,
                end_time=end_time,
            ),
        }
    return metrics


def fetch_ec2_metrics(
    cloudwatch_client,
    ec2_instances: List[Dict[str, object]],
    start_time: datetime,
    end_time: datetime,
) -> Dict[str, Dict[str, Optional[float]]]:
    metrics: Dict[str, Dict[str, Optional[float]]] = {}
    for row in ec2_instances:
        instance_id = row["identifier"]
        dimensions = [{"Name": "InstanceId", "Value": instance_id}]
        metrics[instance_id] = {
            "cpu_average": get_single_metric(
                cloudwatch_client,
                namespace="AWS/EC2",
                metric_name="CPUUtilization",
                dimensions=dimensions,
                start_time=start_time,
                end_time=end_time,
            ),
            "network_in_average_bytes": get_single_metric(
                cloudwatch_client,
                namespace="AWS/EC2",
                metric_name="NetworkIn",
                dimensions=dimensions,
                start_time=start_time,
                end_time=end_time,
            ),
            "network_out_average_bytes": get_single_metric(
                cloudwatch_client,
                namespace="AWS/EC2",
                metric_name="NetworkOut",
                dimensions=dimensions,
                start_time=start_time,
                end_time=end_time,
            ),
        }
    return metrics


def write_outputs(
    output_dir: Path,
    region: str,
    window_start: datetime,
    window_end: datetime,
    rds_rows: List[Dict[str, object]],
    ec2_rows: List[Dict[str, object]],
    rds_metrics: Dict[str, Dict[str, Optional[float]]],
    ec2_metrics: Dict[str, Dict[str, Optional[float]]],
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "database_instance_inventory.json"
    csv_path = output_dir / "database_instance_inventory.csv"

    payload = {
        "region": region,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "rds_instances": rds_rows,
        "ec2_instances": ec2_rows,
        "rds_metrics": rds_metrics,
        "ec2_metrics": ec2_metrics,
    }
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        fieldnames = [
            "instance_type",
            "identifier",
            "engine",
            "engine_version",
            "status",
            "class_or_type",
            "availability_zone",
            "endpoint",
            "allocated_storage_gb",
            "multi_az",
            "storage_encrypted",
            "arn",
            "name_tag",
            "cpu_average",
            "secondary_metric_a",
            "secondary_metric_b",
        ]
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for row in rds_rows:
            db_id = row["identifier"]
            metrics = rds_metrics.get(db_id, {})
            writer.writerow(
                {
                    **row,
                    "name_tag": "",
                    "cpu_average": metrics.get("cpu_average"),
                    "secondary_metric_a": metrics.get("free_storage_space_average_bytes"),
                    "secondary_metric_b": metrics.get("database_connections_average"),
                }
            )

        for row in ec2_rows:
            instance_id = row["identifier"]
            metrics = ec2_metrics.get(instance_id, {})
            writer.writerow(
                {
                    **row,
                    "cpu_average": metrics.get("cpu_average"),
                    "secondary_metric_a": metrics.get("network_in_average_bytes"),
                    "secondary_metric_b": metrics.get("network_out_average_bytes"),
                }
            )

    print(f"Wrote JSON report: {json_path}")
    print(f"Wrote CSV report:  {csv_path}")


def main() -> int:
    args = parse_args()
    try:
        session = build_session(args)
    except ProfileNotFound as error:
        print(build_profile_error_message(args, error))
        return 1

    rds_client = session.client("rds", region_name=args.region)
    ec2_client = session.client("ec2", region_name=args.region)
    cloudwatch_client = session.client("cloudwatch", region_name=args.region)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=args.hours)

    try:
        rds_rows = fetch_rds_instances(rds_client)
        ec2_rows = fetch_ec2_instances(ec2_client)
        rds_metrics = fetch_rds_metrics(cloudwatch_client, rds_rows, start_time, end_time)
        ec2_metrics = fetch_ec2_metrics(cloudwatch_client, ec2_rows, start_time, end_time)
    except ClientError as error:
        print(f"Failed to pull instance inventory: {error}")
        return 1

    write_outputs(
        output_dir=Path(args.output_dir),
        region=args.region,
        window_start=start_time,
        window_end=end_time,
        rds_rows=rds_rows,
        ec2_rows=ec2_rows,
        rds_metrics=rds_metrics,
        ec2_metrics=ec2_metrics,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
