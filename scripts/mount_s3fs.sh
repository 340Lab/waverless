umount /mnt/s3fs
s3fs s3fs /mnt/s3fs -o passwd_file=/root/.passwd-s3fs -o url=http://127.0.0.1:9000     -o use_path_request_style -o umask=0022,uid=$(id -u),gid=$(id -g) -o use_cache=/var/cache/s3fs
echo "mount s3fs success"