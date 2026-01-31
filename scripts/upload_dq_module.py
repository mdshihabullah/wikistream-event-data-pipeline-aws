#!/usr/bin/env python3
"""
Script to package and upload DQ module to S3.
"""
import os
import zipfile
import subprocess
import sys

def main():
    # Create zip file with DQ module
    dq_dir = "spark/jobs/dq"
    zip_path = f"{dq_dir}.zip"
    
    print(f"Packaging DQ module from {dq_dir}...")
    
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        # Add Python files with correct directory structure
        py_files = ['dq_checks.py', 'dq_utils.py', '__init__.py', 'deduplicate_bronze.py']
        for py_file in py_files:
            file_path = os.path.join(dq_dir, py_file)
            if os.path.exists(file_path):
                # Store with dq/ prefix for correct import structure
                zipf.write(file_path, f"dq/{py_file}")
                print(f"  Added: dq/{py_file}")
            else:
                print(f"  Skipping (not found): {py_file}")
    
    print(f"✅ Created: {zip_path}")
    
    # Upload to S3
    bucket = "s3://wikistream-dev-data-160884803380/spark/jobs/dq.zip"
    print(f"\nUploading to S3: {bucket}...")
    
    result = subprocess.run([
        'aws', 's3', 'cp', zip_path, bucket,
        '--profile', os.environ.get('AWS_PROFILE', 'neuefische'),
        '--region', os.environ.get('AWS_REGION', 'us-east-1'),
        '--quiet'
    ], capture_output=True, text=True)
    
    if result.returncode == 0:
        print("✅ Upload successful!")
    else:
        print(f"❌ Upload failed with code {result.returncode}")
        sys.exit(1)

if __name__ == '__main__':
    main()
