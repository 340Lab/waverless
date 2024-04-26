### chdir
import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)

os.system("ansible-playbook -vv 1.ans_build.yml -i ../local_ansible_conf.ini")
