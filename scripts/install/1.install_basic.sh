# 检查命令是否存在
#!/bin/bash

# 定义函数检查并安装命令
install_command() {
    local command_to_check="$1"
    local package_name="$2"

    # 检查命令是否存在
    if command -v "$command_to_check" >/dev/null 2>&1; then
        echo "Command '$command_to_check' is already installed"
    else
        echo "Command '$command_to_check' is not installed, installing now..."

        # 执行安装操作，例如使用包管理器
        # 以下示例使用 apt-get，你可以根据你的系统使用适当的包管理器命令
        apt-get install -y "$package_name"

        # 或者使用其他方式安装，具体取决于你的需求
        # 例如：下载二进制文件，编译源代码等

        echo "Command '$command_to_check' has been installed"
    fi
}

#!/bin/bash

# 函数：检查并安装 Python 包
check_and_install_python_package() {
    local package_name="$1"

    # 检查包是否已安装
    if python3 -c "import $package_name" >/dev/null 2>&1; then
        echo "Python package '$package_name' is already installed."
    else
        echo "Python package '$package_name' is not installed, installing now..."
        
        # 使用 pip 安装包
        python3 -m pip install "$package_name"

        # 或者使用其他方式安装，具体取决于你的需求
        # 例如：使用系统包管理器安装

        echo "Python package '$package_name' has been installed."
    fi
}

# 检查是否以root身份运行
if [ "$EUID" -ne 0 ]; then
    echo "This script must be run as root."
    exit 1
fi

# - clang
# - lldb
# - lld
# - build-essential
# - curl
# - protobuf-compiler
# - pkg-config
# - libssl-dev
# - snap

# 调用函数，将要检查的命令和对应的包名称传递给函数
install_command "python3" "python3"
install_command "pip3" "python3-pip"
install_command "unrar" "unrar"
install_command "tree" "tree"

# 示例：调用函数检查并安装 'requests' 包
check_and_install_python_package "requests"
check_and_install_python_package "ansible" #==4.9.0
check_and_install_python_package "wordlist"
check_and_install_python_package "jsondiff"
check_and_install_python_package "numpy"

apt-get install language-pack-en
locale-gen en_US.UTF-8
