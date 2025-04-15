#!/usr/bin/env python3
import os
import shutil
import argparse
import datetime
import tarfile
from pathlib import Path

def backup_files(directory, file_types=( '.canvas')):
    # Get current timestamp
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Create backup filename
    backup_name = f'backup_{timestamp}.tar.gz'
    backup_path = Path(directory).parent / backup_name
    
    # Create tar archive
    with tarfile.open(backup_path, 'w:gz') as tar:
        # Walk through the directory
        for root, _, files in os.walk(directory):
            # Filter for target file types
            target_files = [f for f in files if f.endswith(file_types)]
            
            for file in target_files:
                file_path = Path(root) / file
                # Add file to archive with its relative path
                tar.add(file_path, arcname=file_path.relative_to(directory))
    
    print(f'Created backup: {backup_path}')
    return backup_path

def sync_md_files(source_dir, target_dir):
    # Convert to Path objects for easier handling
    source_path = Path(source_dir).resolve()
    target_path = Path(target_dir).resolve()
    
    # Create target directory if it doesn't exist
    target_path.mkdir(parents=True, exist_ok=True)
    
    # Counter for statistics
    copied_files = 0
    
    # Walk through the source directory
    for root, _, files in os.walk(source_path):
        # Filter for .md and .canvas files
        target_files = [f for f in files if f.endswith(('.canvas'))]
        
        for target_file in target_files:
            # Get the full source path
            source_file = Path(root) / target_file
            
            # Calculate relative path from source_dir
            rel_path = source_file.relative_to(source_path)
            
            # Create target file path
            target_file = target_path / rel_path
            
            # Create target directory if it doesn't exist
            target_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Copy the file
            shutil.copy2(source_file, target_file)
            copied_files += 1
            print(f"Copied: {rel_path}")
    
    print(f"\nSync complete! Copied {copied_files} Markdown and Canvas files.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Sync markdown and canvas files between local and s3fs')
    parser.add_argument('direction', choices=['to_s3fs', 'from_s3fs'],
                        help='Direction of sync: to_s3fs or from_s3fs')
    args = parser.parse_args()
    
    local_dir = "/root/prjs/waverless"
    s3fs_dir = "/mnt/s3fs/waverless"
    
    if args.direction == 'to_s3fs':
        source_dir = local_dir
        target_dir = s3fs_dir
    else:  # from_s3fs
        source_dir = s3fs_dir
        target_dir = local_dir
    
    # Backup target directory before sync
    print(f"Creating backup of target directory: {target_dir}")
    backup_path = backup_files(target_dir)
    
    print(f"Starting sync from {source_dir} to {target_dir}")
    sync_md_files(source_dir, target_dir)
