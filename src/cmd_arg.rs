use clap::Parser;

/// Simple program to greet a person
#[derive(Parser, Debug)]
// #[command(author, version, about, long_about = None)]
pub struct CmdArgs {
    /// Name of the person to greet
    // #[arg(short, long)]
    pub config_file: String,
}
