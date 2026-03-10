# Local Testing Guide - Metadata Lambda

Full end-to-end testing from your local computer without VPN access. This must be fully tested locally before deploying to AWS Lambda.

## Prerequisites

- Python 3.9+ with required dependencies installed
- AWS CLI configured with `ingest` profile using AWS SSO
- Access to AWS SSM for port forwarding

## Authentication Setup

Before running any tests, authenticate with AWS SSO:
```bash
aws sso login --profile ingest
```

Note: SSO tokens expire periodically - re-run this command if you encounter authentication errors.

## Setup Development Directory

Create the development directory structure:

```bash
mkdir -p ../dev
```

## Local Testing Workflow

### 1. Basic Local Test (No Database Connection)

First, test the core functionality locally without connecting to the database:

```bash
python test_meta_handler.py
```

The test script (`test_meta_handler.py`) should look like:

```python
import os
import json
import time

# Set environment variables BEFORE importing handler
os.environ['AWS_PROFILE'] = 'ingest'
os.environ['DEV'] = 'True'
os.environ['LOCAL_RUN'] = 'True'
os.environ['LOG_LEVEL'] = 'DEBUG'
os.environ['INTERNAL_BUCKET_NAME'] = "dummy-ingest-bucket"

# Import AFTER setting environment variables
from meta_handler import main
from args import args

# Run the function - logs output to ../dev folder
response = main()
```

**What this does:**
- Tests core `meta_handler` functionality
- Runs in local development mode with debug logging
- Outputs logs and results to `../dev` folder
- Allows quick verification before connecting to database

### 2. Setup AWS SSM Database Tunnel

To test with actual database access, set up port forwarding tunnel to the metadata database. Run this command in a separate terminal window and keep it running:

```bash
aws ssm start-session \
  --target i-095f2bbb20b7d81fc \
  --document-name AWS-StartPortForwardingSessionToRemoteHost \
  --parameters '{"host":["mothmetadata.read.db.int.synopticdata.net"],"portNumber":["3306"],"localPortNumber":["3306"]}'
```

#### 2.1 -   Local Lambda / Python Execution (Non-Docker) Hostname Mapping

When running your Lambda function locally in **production mode** (`LOCAL_RUN=False`), you'll need to configure hostname mapping to connect to the production database through your SSM tunnel.

#####  Why This Is Required

- `ingestlib.station_lookup` and other production code connects to `mothmetadata.read.db.int.synopticdata.net`
- This hostname does not resolve on your local machine
- Your SSM tunnel listens on `127.0.0.1:3306`
- To make production-mode code work locally, you must map the hostname to `localhost` so the code connects to your tunnel instead of trying to reach the internal hostname directly

#####  Setup Commands

Add the hostname mapping to your `/etc/hosts` file:
```bash
sudo sh -c 'printf "\n127.0.0.1 mothmetadata.read.db.int.synopticdata.net\n" >> /etc/hosts'
```

Flush the DNS cache (macOS):
```bash
sudo dscacheutil -flushcache
sudo killall -HUP mDNSResponder
```

##### Verify the Mapping

Run this command to confirm the hostname now resolves to `127.0.0.1`:
```bash
python -c "import socket; print(socket.gethostbyname('mothmetadata.read.db.int.synopticdata.net'))"
```

**Expected output:**
```
127.0.0.1
```

> ** Important Notes**
> 
> - This setup is **only required** when:
>   - Running locally (not in AWS)
>   - Using `LOCAL_RUN=False` (production database behavior)
>   - Running Python directly (not in Docker)
> - **Docker users do not need this** - Docker handles hostname mapping automatically with `--add-host` (see Docker section)
> - Your SSM tunnel **must remain open** while running the script
> - This configuration does not affect AWS or production environments

##### Cleanup (Optional)

To remove the hostname mapping later, manually delete the line from `/etc/hosts`:
```bash
sudo nano /etc/hosts
# Delete the line: 127.0.0.1 mothmetadata.read.db.int.synopticdata.net
```

Then flush the DNS cache again.
**Important:** Keep this tunnel session running while testing database connectivity.

### 3. Run Full Local Test with Database Access

Update your test script to connect to the database:

```python
import os
import json
import time

# Set environment variables for full testing with database
os.environ['AWS_PROFILE'] = 'ingest'
os.environ['DEV'] = 'False'
os.environ['LOCAL_RUN'] = 'False'
os.environ['LOG_LEVEL'] = 'DEBUG'
os.environ['DB_HOST'] = '127.0.0.1'
os.environ['DB_PORT'] = '3306'
os.environ['INTERNAL_BUCKET_NAME'] = 'dummy-ingest-bucket'
os.environ['AWS_REGION'] = 'us-west-2'

# Import AFTER setting environment variables
from meta_handler import main
from args import args

# Run the function
response = main()
```

Execute the test:

```bash
python test_meta_handler.py
```

### 4. Validate Results

Review outputs in `../dev/`:
- `stations_metadata.json` - Station metadata for review
- `*.log` - Detailed execution logs
- `*.sql` - Generated SQL scripts for database updates

Verify:
- Stations are correctly formatted in JSON output
- Lat/lon/elevation values are accurate
- SQL script is valid and ready for execution
- All validation checks pass

### 5. Add Generated Stations to Database

After validating the metadata:
1. Review the generated `.sql` file in `../dev/`
2. Execute the script in the dev database first
3. Verify stations appear correctly
4. Execute in production database

## Environment Variables Reference

| Variable | Description | Example Value |
|----------|-------------|---------------|
| `DEV` | Development mode flag | `True`/`False` |
| `LOCAL_RUN` | Local execution flag | `True`/`False` |
| `LOG_LEVEL` | Logging verbosity | `DEBUG`/`INFO`/`WARNING`/`ERROR` |
| `INTERNAL_BUCKET_NAME` | S3 bucket name | `dummy-ingest-bucket` |
| `AWS_REGION` | AWS region | `us-west-2` |

## Configuration Constants

Update these constants in `meta_handler.py` before testing:

- `INGEST_NAME` - Name of your ingest source
- `MNET_ID`, `MNET_SHORTNAME` - Network identifiers
- `STID_PREFIX` - Station ID prefix for your network
- `ELEVATION_UNIT` - "METERS" or "FEET"
- `RESTRICTED_DATA_STATUS`, `RESTRICTED_METADATA_STATUS` - Access restrictions

## Troubleshooting

### Common Issues

1. **Import errors or missing dependencies**:
   - Ensure all required Python packages are installed
   - Check that you're in the correct directory

2. **AWS authentication errors**:
   - Run `aws sso login --profile ingest` to refresh token
   - Verify your ingest profile is configured correctly

3. **Database connection failures**:
   - Verify AWS SSM tunnel is running in separate terminal
   - Check that port 3306 is not already in use
   - Ensure tunnel target instance is accessible

4. **Missing output files**:
   - Ensure `../dev/` directory exists
   - Check file permissions

5. **Station metadata validation failures**:
   - Review debug logs in `../dev/*.log`
   - Verify source data format is correct
   - Check lat/lon/elevation conversions

### Debug Commands

```bash
# Verify AWS authentication
aws sts get-caller-identity --profile ingest

# Check SSM tunnel status
netstat -an | grep 3306

# Test database connectivity (if mysql client installed)
mysql -h 127.0.0.1 -P 3306 -u username -p
```

## Next Steps

After successful local testing:
1. Review generated SQL and metadata JSON files
2. Execute SQL in dev database, then prod
3. Deploy to AWS using CDK (see main README)
4. Monitor CloudWatch logs for production behavior

## Important Notes

- **Never overwrite existing STIDs in MetaManager once deployed**
- Keep AWS SSM tunnel active during database testing
- Always test metadata generation fully before deployment
- Clean up `.sql` files from `../dev/` after database execution
- SSO tokens expire - re-authenticate if you see credential errors