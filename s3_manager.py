"""
============================================================
AWS S3 Manager - Global Air Quality Dynamics Project
============================================================
Handles all interactions with Amazon S3:
  - Bucket creation and configuration
  - Multi-part upload of large CSV files
  - Download and retrieval of data
  - Listing and managing stored objects
  - Generating pre-signed URLs for sharing

Usage:
    python s3_manager.py upload      # Upload all data files to S3
    python s3_manager.py download    # Download data files from S3
    python s3_manager.py list        # List all objects in S3 bucket
    python s3_manager.py status      # Show bucket status and metrics
    python s3_manager.py cleanup     # Delete all objects from S3

IMPORTANT: Configure your .env file with AWS credentials first!
============================================================
"""

import os
import sys
import json
import time
import hashlib
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
from tqdm import tqdm

from config import AWSConfig, DatasetConfig, ProjectInfo


class S3Manager:
    """
    Manages AWS S3 operations for the air quality dataset,
    including multi-part uploads for large files.
    """

    def __init__(self):
        """Initialize S3 client with credentials from .env"""
        if not AWSConfig.validate():
            print("\nPlease configure your AWS credentials in the .env file.")
            print("   See README.md for setup instructions.")
            sys.exit(1)

        self.session = boto3.Session(
            aws_access_key_id=AWSConfig.ACCESS_KEY_ID,
            aws_secret_access_key=AWSConfig.SECRET_ACCESS_KEY,
            region_name=AWSConfig.REGION
        )

        if AWSConfig.ENDPOINT_URL:
            self.s3_client = self.session.client(
                's3', endpoint_url=AWSConfig.ENDPOINT_URL)
            self.s3_resource = self.session.resource(
                's3', endpoint_url=AWSConfig.ENDPOINT_URL)
        else:
            self.s3_client = self.session.client('s3')
            self.s3_resource = self.session.resource('s3')

        self.bucket_name = AWSConfig.BUCKET_NAME
        self.prefix = AWSConfig.DATA_PREFIX
        self.chunk_size = AWSConfig.UPLOAD_CHUNK_SIZE_MB * 1024 * 1024

        print(f" AWS S3 client initialized")
        print(f"   Region: {AWSConfig.REGION}")
        print(f"   Bucket: {self.bucket_name}")

    # --------------------------------------------------------
    # Bucket Operations
    # --------------------------------------------------------

    def create_bucket(self):
        """Create S3 bucket if it doesn't exist."""
        try:
            self.s3_client.head_bucket(Bucket=self.bucket_name)
            print(f" Bucket '{self.bucket_name}' already exists")
            return True
        except ClientError as e:
            error_code = int(e.response['Error']['Code'])
            if error_code == 404:
                try:
                    if AWSConfig.REGION == 'us-east-1':
                        self.s3_client.create_bucket(Bucket=self.bucket_name)
                    else:
                        self.s3_client.create_bucket(
                            Bucket=self.bucket_name,
                            CreateBucketConfiguration={
                                'LocationConstraint': AWSConfig.REGION
                            }
                        )

                    # Wait for bucket to exist
                    waiter = self.s3_client.get_waiter('bucket_exists')
                    waiter.wait(Bucket=self.bucket_name)

                    # Enable versioning
                    self.s3_client.put_bucket_versioning(
                        Bucket=self.bucket_name,
                        VersioningConfiguration={'Status': 'Enabled'}
                    )

                    # Add tags
                    self.s3_client.put_bucket_tagging(
                        Bucket=self.bucket_name,
                        Tagging={
                            'TagSet': [
                                {'Key': 'Project', 'Value': 'Global-Air-Quality-Dynamics'},
                                {'Key': 'Module', 'Value': ProjectInfo.MODULE_CODE},
                                {'Key': 'Purpose', 'Value': 'Air-Quality-Dataset-Storage'},
                            ]
                        }
                    )

                    print(f" Bucket '{self.bucket_name}' created successfully")
                    return True
                except ClientError as ce:
                    print(f" Failed to create bucket: {ce}")
                    return False
            elif error_code == 403:
                print(f" Access denied to bucket '{self.bucket_name}'")
                return False
            else:
                print(f" Error checking bucket: {e}")
                return False

    def configure_bucket_lifecycle(self):
        """Set up lifecycle rules for cost optimization."""
        try:
            lifecycle_config = {
                'Rules': [
                    {
                        'ID': 'TransitionToIA',
                        'Filter': {'Prefix': self.prefix},
                        'Status': 'Enabled',
                        'Transitions': [
                            {
                                'Days': 30,
                                'StorageClass': 'STANDARD_IA'
                            },
                            {
                                'Days': 90,
                                'StorageClass': 'GLACIER'
                            }
                        ]
                    },
                    {
                        'ID': 'CleanupIncompleteUploads',
                        'Filter': {'Prefix': ''},
                        'Status': 'Enabled',
                        'AbortIncompleteMultipartUpload': {
                            'DaysAfterInitiation': 7
                        }
                    }
                ]
            }

            self.s3_client.put_bucket_lifecycle_configuration(
                Bucket=self.bucket_name,
                LifecycleConfiguration=lifecycle_config
            )
            print("Lifecycle rules configured (IA after 30d, Glacier after 90d)")
        except ClientError as e:
            print(f" Could not set lifecycle rules: {e}")

    # --------------------------------------------------------
    # Upload Operations
    # --------------------------------------------------------

    def _compute_file_hash(self, file_path):
        """Compute SHA256 hash of a file."""
        sha256 = hashlib.sha256()
        with open(file_path, 'rb') as f:
            for block in iter(lambda: f.read(8192), b''):
                sha256.update(block)
        return sha256.hexdigest()

    def upload_file(self, file_path, s3_key=None, show_progress=True):
        """
        Upload a single file to S3 with multi-part upload for large files.
        """
        file_path = Path(file_path)
        if not file_path.exists():
            print(f" File not found: {file_path}")
            return False

        if s3_key is None:
            s3_key = f"{self.prefix}{file_path.name}"

        file_size = file_path.stat().st_size
        file_hash = self._compute_file_hash(file_path)

        # Metadata
        metadata = {
            'project': ProjectInfo.TITLE,
            'sha256': file_hash,
            'original-filename': file_path.name,
            'upload-timestamp': datetime.now().isoformat(),
            'file-size-bytes': str(file_size)
        }

        try:
            if show_progress:
                pbar = tqdm(
                    total=file_size,
                    unit='B',
                    unit_scale=True,
                    desc=f"     {file_path.name}",
                    bar_format='{l_bar}{bar:30}{r_bar}'
                )

                def progress_callback(bytes_transferred):
                    pbar.update(bytes_transferred)

                # Use multipart upload config for large files
                from boto3.s3.transfer import TransferConfig
                config = TransferConfig(
                    multipart_threshold=self.chunk_size,
                    multipart_chunksize=self.chunk_size,
                    max_concurrency=AWSConfig.UPLOAD_WORKERS,
                    use_threads=True
                )

                self.s3_client.upload_file(
                    str(file_path),
                    self.bucket_name,
                    s3_key,
                    Config=config,
                    Callback=progress_callback,
                    ExtraArgs={
                        'Metadata': metadata,
                        'ContentType': 'text/csv' if file_path.suffix == '.csv'
                                       else 'application/json',
                        'ServerSideEncryption': 'AES256'
                    }
                )
                pbar.close()
            else:
                self.s3_client.upload_file(
                    str(file_path),
                    self.bucket_name,
                    s3_key,
                    ExtraArgs={
                        'Metadata': metadata,
                        'ServerSideEncryption': 'AES256'
                    }
                )

            return True

        except ClientError as e:
            print(f" Upload failed for {file_path.name}: {e}")
            return False

    def upload_all_data(self):
        """Upload all generated data files to S3."""
        data_dir = DatasetConfig.DATA_DIR

        if not data_dir.exists():
            print(" Data directory not found. Run data_generator.py first!")
            return False

        files = sorted(data_dir.glob('*.csv')) + sorted(data_dir.glob('*.json'))

        if not files:
            print(" No data files found. Run data_generator.py first!")
            return False

        print(f"\n{'='*60}")
        print(f"  ⬆  UPLOADING DATA TO AWS S3")
        print(f"{'='*60}")
        print(f"   Bucket:  {self.bucket_name}")
        print(f"   Files:   {len(files)}")
        total_size = sum(f.stat().st_size for f in files)
        print(f"   Total:   {total_size / (1024**3):.2f} GB")
        print(f"{'='*60}\n")

        # Create bucket if needed
        if not self.create_bucket():
            return False

        # Configure lifecycle rules
        self.configure_bucket_lifecycle()

        start_time = time.time()
        successful = 0
        failed = 0

        for file_path in files:
            success = self.upload_file(file_path)
            if success:
                successful += 1
            else:
                failed += 1

        elapsed = time.time() - start_time

        print(f"\n{'='*60}")
        print(f"  UPLOAD COMPLETE")
        print(f"{'='*60}")
        print(f"   Successful: {successful}")
        print(f"   Failed:     {failed}")
        print(f"    Time:      {elapsed:.1f} seconds")
        print(f"   Speed:     {total_size / elapsed / (1024**2):.1f} MB/s")
        print(f"{'='*60}\n")

        # Upload a manifest file
        self._upload_manifest(files)

        return failed == 0

    def _upload_manifest(self, files):
        """Create and upload a manifest of all uploaded files."""
        manifest = {
            'project': ProjectInfo.TITLE,
            'upload_timestamp': datetime.now().isoformat(),
            'bucket': self.bucket_name,
            'prefix': self.prefix,
            'files': []
        }

        for f in files:
            manifest['files'].append({
                'name': f.name,
                's3_key': f"{self.prefix}{f.name}",
                'size_bytes': f.stat().st_size,
                'sha256': self._compute_file_hash(f)
            })

        manifest_path = DatasetConfig.DATA_DIR / 'upload_manifest.json'
        with open(manifest_path, 'w') as mf:
            json.dump(manifest, mf, indent=2)

        self.upload_file(manifest_path, f"{self.prefix}upload_manifest.json",
                         show_progress=False)
        print(" Upload manifest saved and uploaded")

    # --------------------------------------------------------
    # Download Operations
    # --------------------------------------------------------

    def download_file(self, s3_key, local_path=None, show_progress=True):
        """Download a file from S3."""
        if local_path is None:
            local_path = DatasetConfig.DATA_DIR / os.path.basename(s3_key)

        local_path = Path(local_path)
        local_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            # Get file size for progress bar
            response = self.s3_client.head_object(
                Bucket=self.bucket_name, Key=s3_key)
            file_size = response['ContentLength']

            if show_progress:
                pbar = tqdm(
                    total=file_size,
                    unit='B',
                    unit_scale=True,
                    desc=f"     {os.path.basename(s3_key)}",
                    bar_format='{l_bar}{bar:30}{r_bar}'
                )

                def progress_callback(bytes_transferred):
                    pbar.update(bytes_transferred)

                self.s3_client.download_file(
                    self.bucket_name, s3_key, str(local_path),
                    Callback=progress_callback
                )
                pbar.close()
            else:
                self.s3_client.download_file(
                    self.bucket_name, s3_key, str(local_path))

            return True

        except ClientError as e:
            print(f" Download failed: {e}")
            return False

    def download_all_data(self):
        """Download all data files from S3."""
        print(f"\n{'='*60}")
        print(f"    DOWNLOADING DATA FROM AWS S3")
        print(f"{'='*60}\n")

        DatasetConfig.ensure_directories()
        objects = self.list_objects()

        if not objects:
            print(" No objects found in bucket")
            return False

        start_time = time.time()
        successful = 0

        for obj in objects:
            local_path = DatasetConfig.DATA_DIR / os.path.basename(obj['Key'])
            if self.download_file(obj['Key'], local_path):
                successful += 1

        elapsed = time.time() - start_time
        print(f"\n Downloaded {successful}/{len(objects)} files "
              f"in {elapsed:.1f}s")
        return True

    # --------------------------------------------------------
    # Listing & Status
    # --------------------------------------------------------

    def list_objects(self, prefix=None):
        """List all objects in the bucket."""
        if prefix is None:
            prefix = self.prefix

        objects = []
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            for page in paginator.paginate(Bucket=self.bucket_name,
                                            Prefix=prefix):
                if 'Contents' in page:
                    objects.extend(page['Contents'])
        except ClientError as e:
            print(f" Error listing objects: {e}")

        return objects

    def show_bucket_status(self):
        """Display detailed bucket status."""
        print(f"\n{'='*60}")
        print(f"   S3 BUCKET STATUS")
        print(f"{'='*60}")

        objects = self.list_objects()
        total_size = sum(obj['Size'] for obj in objects)

        print(f"   Bucket:          {self.bucket_name}")
        print(f"   Region:          {AWSConfig.REGION}")
        print(f"   Total objects:   {len(objects)}")
        print(f"   Total size:      {total_size / (1024**3):.2f} GB")
        print(f"   Avg object size: "
              f"{total_size / len(objects) / (1024**2):.1f} MB"
              if objects else "   Avg object size: N/A")

        if objects:
            print(f"\n    Objects:")
            for obj in objects:
                size_mb = obj['Size'] / (1024 * 1024)
                last_mod = obj['LastModified'].strftime('%Y-%m-%d %H:%M')
                print(f"     • {obj['Key']:<50} "
                      f"{size_mb:>8.1f} MB  ({last_mod})")

        print(f"{'='*60}\n")

    def generate_presigned_urls(self, expiry_hours=24):
        """Generate pre-signed URLs for sharing data."""
        objects = self.list_objects()
        urls = {}

        for obj in objects:
            try:
                url = self.s3_client.generate_presigned_url(
                    'get_object',
                    Params={
                        'Bucket': self.bucket_name,
                        'Key': obj['Key']
                    },
                    ExpiresIn=expiry_hours * 3600
                )
                urls[obj['Key']] = url
            except ClientError as e:
                print(f"  Could not generate URL for {obj['Key']}: {e}")

        return urls

    # --------------------------------------------------------
    # Cleanup
    # --------------------------------------------------------

    def cleanup_bucket(self):
        """Delete all objects from the bucket."""
        print(f"\n  Cleaning up bucket: {self.bucket_name}")

        objects = self.list_objects()
        if not objects:
            print("   Bucket is already empty")
            return

        # Delete objects in batches
        delete_objects = [{'Key': obj['Key']} for obj in objects]
        batch_size = 1000

        for i in range(0, len(delete_objects), batch_size):
            batch = delete_objects[i:i + batch_size]
            self.s3_client.delete_objects(
                Bucket=self.bucket_name,
                Delete={'Objects': batch}
            )
            print(f"     Deleted batch: {len(batch)} objects")

        print(f" Deleted {len(objects)} objects from bucket")


# ============================================================
# CLI Interface
# ============================================================

def main():
    """Command-line interface for S3 operations."""
    if len(sys.argv) < 2:
        print("""
╔══════════════════════════════════════════════════════╗
║  AWS S3 Manager - Air Quality Dataset               ║
╠══════════════════════════════════════════════════════╣
║                                                      ║
║  Usage: python s3_manager.py <command>               ║
║                                                      ║
║  Commands:                                           ║
║    upload    - Upload all data files to S3            ║
║    download  - Download data files from S3            ║
║    list      - List all objects in S3 bucket          ║
║    status    - Show bucket status and metrics         ║
║    urls      - Generate pre-signed download URLs      ║
║    cleanup   - Delete all objects from bucket         ║
║                                                      ║
╚══════════════════════════════════════════════════════╝
        """)
        return

    command = sys.argv[1].lower()
    manager = S3Manager()

    if command == 'upload':
        manager.upload_all_data()
    elif command == 'download':
        manager.download_all_data()
    elif command == 'list':
        manager.show_bucket_status()
    elif command == 'status':
        manager.show_bucket_status()
    elif command == 'urls':
        urls = manager.generate_presigned_urls()
        print("\n🔗 Pre-signed URLs (valid for 24 hours):\n")
        for key, url in urls.items():
            print(f"   {key}:")
            print(f"   {url}\n")
    elif command == 'cleanup':
        confirm = input("  This will DELETE all data. Type 'YES' to confirm: ")
        if confirm == 'YES':
            manager.cleanup_bucket()
        else:
            print("Cancelled.")
    else:
        print(f" Unknown command: {command}")
        print("   Run 'python s3_manager.py' for usage info")


if __name__ == '__main__':
    main()
