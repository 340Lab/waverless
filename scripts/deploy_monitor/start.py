import os

CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)

# chdir to the directory of this script
os.chdir(CUR_FDIR)

# docker compose up
os.system('bash ../install/install_docker.sh')
os.system('docker-compose up -d')