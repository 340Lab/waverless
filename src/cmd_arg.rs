use clap::Parser;

use crate::sys::NodeID;

/// Simple program to greet a person
#[derive(Parser, Debug)]
// #[command(author, version, about, long_about = None)]
pub struct CmdArgs {
    /// Name of the person to greet
    // #[arg(short, long)]
    // pub config_file: String,
    pub this_id: NodeID,
}
