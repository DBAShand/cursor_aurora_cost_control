# cursor_aurora_cost_control

Yes, this is possible. This repository now includes two separate scripts:

1. `scripts/pull_billing_cloudwatch_reports.py`  
   - Intended for the **Billing** role.
   - Pulls billing-related CloudWatch metrics (`AWS/Billing`, `EstimatedCharges`) and exports:
     - JSON report
     - CSV report

2. `scripts/pull_database_instance_inventory.py`  
   - Intended for the **DatabaseAdministrator** role.
   - Pulls RDS + EC2 instance inventory plus recent CloudWatch health metrics and exports:
     - JSON report
     - CSV report

## Requirements

- Python 3.9+
- `boto3`
- AWS credentials configured either as:
  - a named AWS CLI profile (`--profile`), or
  - an assumable role ARN (`--role-arn`)

Install dependency if needed:

```bash
python3 -m pip install boto3
```

## Billing script (Billing role)

### Example: run with named profile

```bash
python3 scripts/pull_billing_cloudwatch_reports.py \
  --profile billing \
  --hours 720 \
  --output-dir output/billing
```

### Example: run by assuming role

```bash
python3 scripts/pull_billing_cloudwatch_reports.py \
  --role-arn arn:aws:iam::123456789012:role/BillingRole \
  --hours 720 \
  --output-dir output/billing
```

### Output files

- `output/billing/billing_cloudwatch_report.json`
- `output/billing/billing_cloudwatch_report.csv`

## Database script (DatabaseAdministrator role)

### Example: run with named profile

```bash
python3 scripts/pull_database_instance_inventory.py \
  --profile databaseadmin \
  --region us-east-1 \
  --hours 24 \
  --output-dir output/database
```

### Example: run by assuming role

```bash
python3 scripts/pull_database_instance_inventory.py \
  --role-arn arn:aws:iam::123456789012:role/DatabaseAdministrator \
  --region us-east-1 \
  --hours 24 \
  --output-dir output/database
```

### Output files

- `output/database/database_instance_inventory.json`
- `output/database/database_instance_inventory.csv`

## Notes

- Billing metrics are read from CloudWatch in `us-east-1` (`AWS/Billing` namespace).
- The database script queries a single region at a time; run it per region if needed.
- If your organization requires an external ID for role assumption, pass `--external-id`.
