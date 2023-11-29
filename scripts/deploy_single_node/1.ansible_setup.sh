ssh-keygen -t ed25519 -f ~/.ssh/whatever -N ''
cat > ~/.ssh/config <<EOF
    Host host.example
    User $USER
    HostName 127.0.0.1
    IdentityFile ~/.ssh/whatever
EOF
echo -n 'from="127.0.0.1" ' | cat - ~/.ssh/whatever.pub > ~/.ssh/authorized_keys
echo "Before fixing permissions on authorized_keys, notice home directory is world read/write"
ls -la ~/.ssh
ssh -o 'StrictHostKeyChecking no' host.example id || echo "ssh failed as expected... trying to fix permissions"
chmod og-rw ~
echo "After fixing permissions on home folder ~ ..."
ls -la ~/.ssh
ssh -o 'StrictHostKeyChecking no' host.example id