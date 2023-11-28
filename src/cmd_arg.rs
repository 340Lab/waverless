use clap::Parser;

use crate::sys::NodeID;

/// Simple program to greet a person
#[derive(Parser, Debug)]
// #[command(author, version, about, long_about = None)]
pub struct CmdArgs {
    /// Name of the person to greet
    // #[arg(short, long)]
    pub this_id: NodeID,
    pub config_file: String,
    // wrap password
    // pub deploy: Option<String>,
}
