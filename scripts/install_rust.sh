apt install clang lldb lld
apt install build-essential
apt install curl


# if rust not installed
if ! command -v rustup &> /dev/null
then
    echo "rust not installed"
    # install rust
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
    # add rust to path
    source "$HOME/.cargo/env"
fi


rustup target add wasm32-unknown-unknown
rustup default 1.70