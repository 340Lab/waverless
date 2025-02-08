#!/usr/bin/env python3
import os
import shutil
import argparse
import datetime
import tarfile
from pathlib import Path


def sync_md_files(source_dir, target_dir):
    # read source file
    toreplace="	"
    withcontent="  "
    with open(f"{source_dir}/design.canvas") as f:
        canvas = f.read()
        canvas=canvas.replace(toreplace,withcontent)
    with open(f"{source_dir}/design.canvas","w") as f:
        f.write(canvas)

    os.system(f"cp -r {source_dir}/design.canvas {target_dir}/design.canvas")

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
    
    # # Backup target directory before sync
    # print(f"Creating backup of target directory: {target_dir}")
    # backup_path = backup_files(target_dir)
    
    print(f"Starting sync from {source_dir} to {target_dir}")
    sync_md_files(source_dir, target_dir)
    if args.direction == 'from_s3fs':
        timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        os.system(f"cp {target_dir}/design.canvas {target_dir}/design.canvas.{timestamp}.bak")
        print(f"Backup design.canvas to design.canvas.{timestamp}.bak")
