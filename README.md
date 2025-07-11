# BYOC Data Ingestion Tool

This tool enables users to easily ingest data into a BYOC (Bring Your Own Collection) in Sentinel Hub / Copernicus Data Space Ecosystem.

## Installation

1. Clone this repository
2. Choose one of the following methods to install the required dependencies:

### Option 1: Using pip with requirements.txt

```bash
pip install -r requirements.txt
```

### Option 2: Using conda with environment.yml

```bash
conda env create -f environment.yml
conda activate sentinel-byoc
```

## Configuration

The tool uses a `config.json` file to securely store your credentials and configuration parameters. 

### Setting up config.json

Create a `config.json` file in the root directory of the project with the following structure:

```json
{
  "sentinel_hub_client_id": "your-sentinel-hub-client-id",
  "sentinel_hub_client_secret": "your-sentinel-hub-client-secret",
  "s3_username": "your-s3-username",
  "s3_password": "your-s3-password",
  "bucket_url": "your-s3-bucket-url"
}
```

Replace the placeholder values with your actual credentials:

1. **CDSE Sentinel Hub OAuth Credentials**
   - `sentinel_hub_client_id`: Your Sentinel Hub Client ID
   - `sentinel_hub_client_secret`: Your Sentinel Hub Client Secret

2. **S3 Storage Access Credentials**
   - `s3_username`: Your S3 storage username
   - `s3_password`: Your S3 storage password
   - `bucket_url`: The URL of your S3 bucket (e.g., "eodata.dataspace.copernicus.eu")


## Usage

You can run the ingestion pipeline by using the provided Jupyter Notebook: `BYOC_Data_Ingestion.ipynb`.

