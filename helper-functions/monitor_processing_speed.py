#!/usr/bin/env python3
import subprocess
import time
import sys

def run_s3_counter():
    """Run the S3 file counter and return txt file count"""
    result = subprocess.run(
        [sys.executable, "/Users/morrissimons/Desktop/Podcast scanner/helper-functions/how-many-txt-files-in-scaleway.py"],
        capture_output=True,
        text=True
    )
    if result.returncode != 0:
        raise Exception(f"S3 counter failed: {result.stderr}")
    
    for line in result.stdout.split('\n'):
        if line.startswith('  Total .txt files:'):
            return int(line.split(':')[1].strip().replace(',', ''))
    raise Exception("Could not parse txt file count from output")

def main():
    print("Getting initial txt file count...")
    initial_count = run_s3_counter()
    print(f"Initial count: {initial_count:,} files\n")
    
    print("Waiting 5 minutes...")
    time.sleep(300)
    
    print("\nGetting final txt file count...")
    final_count = run_s3_counter()
    print(f"Final count: {final_count:,} files\n")
    
    new_files = final_count - initial_count
    print("=" * 50)
    print(f"New txt files added: {new_files:,}")
    print("=" * 50)

if __name__ == "__main__":
    main()