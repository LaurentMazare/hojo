use anyhow::Result;
use clap::Parser;

use pyo3::prelude::*;

#[derive(Debug, Parser)]
struct Args {
    /// The socket file used for communication.
    #[arg(long)]
    socket_file: String,
}

fn main() -> Result<()> {
    unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL);
    }
    env_logger::builder().filter_level(log::LevelFilter::Info).init();
    log::info!("starting the hojo subprocess");
    let args = Args::parse();
    Python::with_gil(|py| hojo::run_server(&args.socket_file, py))
}
