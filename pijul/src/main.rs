mod commands;
mod config;
mod progress;
mod remote;
mod repository;

use std::ffi::OsString;
use std::io::Write;
use std::path::PathBuf;

use anyhow::bail;
use clap::{AppSettings, Clap};
use human_panic::setup_panic;

use crate::commands::*;

const DEFAULT_CHANNEL: &str = "main";
const PROTOCOL_VERSION: usize = 3;

#[derive(Clap, Debug)]
#[clap(
    version,
    author,
    global_setting(AppSettings::ColoredHelp),
    setting(AppSettings::InferSubcommands)
)]
pub struct Opts {
    #[clap(subcommand)]
    pub subcmd: SubCommand,
}

#[derive(Clap, Debug)]
pub enum SubCommand {
    /// Initializes an empty pijul repository
    Init(Init),

    /// Clones an existing pijul repository
    Clone(Clone),

    /// Creates a new change
    Record(Record),

    /// Shows difference between two channels/changes
    Diff(Diff),

    /// Show the entire log of changes
    Log(Log),

    /// Pushes changes to a remote upstream
    Push(Push),

    /// Pulls changes from a remote upstream
    Pull(Pull),

    /// Shows information about a particular change
    Change(Change),

    /// Manages different channels
    Channel(Channel),

    #[clap(setting = AppSettings::Hidden)]
    Protocol(Protocol),

    #[cfg(feature = "git")]
    /// Imports a git repository into pijul
    Git(Git),

    /// Moves a file in the working copy and the tree
    #[clap(alias = "mv")]
    Move(Move),

    /// Lists files tracked by pijul
    #[clap(alias = "ls")]
    List(List),

    /// Adds a path to the tree.
    ///
    /// Pijul has an internal tree to represent the files currently
    /// tracked. This command adds files and directories to that tree.
    Add(Add),

    /// Removes a file from the tree and pristine
    #[clap(alias = "rm")]
    Remove(Remove),

    /// Resets the working copy to the last recorded change.
    ///
    /// In other words, discards all unrecorded changes.
    Reset(Reset),

    // #[cfg(debug_assertions)]
    Debug(Debug),

    /// Create a new channel
    Fork(Fork),

    /// Unrecords a list of changes.
    ///
    /// The changes will be removed from your log, but your working
    /// copy will stay exactly the same, unless the
    /// `--reset` flag was passed. A change can only be unrecorded
    /// if all changes that depend on it are also unrecorded in the
    /// same operation. There are two ways to call `pijul-unrecord`:
    ///
    /// * With a list of <change-id>s. The given changes will be
    /// unrecorded, if possible.
    ///
    /// * Without listing any <change-id>s. You will be
    /// presented with a list of changes to choose from.
    /// The length of the list is determined by the `unrecord_changes`
    /// setting in your global config or the `--show-changes` option,
    /// with the latter taking precedence.
    Unrecord(Unrecord),

    /// Applies changes to a channel
    Apply(Apply),

    /// Manages remote repositories
    Remote(Remote),

    /// Creates an archive of the repository
    Archive(Archive),

    /// Shows which patch last affected each line of the every file
    Credit(Credit),

    /// Manage tags (create tags, check out a tag)
    Tag(Tag),

    #[clap(external_subcommand)]
    ExternalSubcommand(Vec<OsString>),
}

#[tokio::main]
async fn main() {
    setup_panic!();
    env_logger::init();

    let opts = Opts::parse();

    if let Err(e) = run(opts).await {
        match e.downcast::<std::io::Error>() {
            Ok(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {}
            Ok(e) => writeln!(std::io::stderr(), "Error: {}", e).unwrap_or(()),
            Err(e) => writeln!(std::io::stderr(), "Error: {}", e).unwrap_or(()),
        }
        std::process::exit(1);
    } else {
        std::process::exit(0);
    }
}

#[cfg(unix)]
fn run_external_command(mut command: Vec<OsString>) -> Result<(), std::io::Error> {
    let args = command.split_off(1);
    let mut cmd: OsString = "pijul-".into();
    cmd.push(&command[0]);

    use std::os::unix::process::CommandExt;
    let err = std::process::Command::new(&cmd).args(args).exec();
    report_external_command_error(&command[0], err);
}

#[cfg(windows)]
fn run_external_command(mut command: Vec<OsString>) -> Result<(), std::io::Error> {
    let args = command.split_off(1);
    let mut cmd: OsString = "pijul-".into();
    cmd.push(&command[0]);

    let mut spawned = match std::process::Command::new(&cmd).args(args).spawn() {
        Ok(spawned) => spawned,
        Err(e) => {
            report_external_command_error(&command[0], e);
        }
    };
    let status = spawned.wait()?;
    std::process::exit(status.code().unwrap_or(1))
}

fn report_external_command_error(cmd: &OsString, err: std::io::Error) -> ! {
    if err.kind() == std::io::ErrorKind::NotFound {
        writeln!(std::io::stderr(), "No such subcommand: {:?}", cmd).unwrap_or(());
    } else {
        writeln!(std::io::stderr(), "Error while running {:?}: {}", cmd, err).unwrap_or(());
    }
    std::process::exit(1)
}

async fn run(opts: Opts) -> Result<(), anyhow::Error> {
    match opts.subcmd {
        SubCommand::Log(l) => l.run().await,
        SubCommand::Init(init) => init.run().await,
        SubCommand::Clone(clone) => clone.run().await,
        SubCommand::Record(record) => record.run().await,
        SubCommand::Diff(diff) => diff.run().await,
        SubCommand::Push(push) => push.run().await,
        SubCommand::Pull(pull) => pull.run().await,
        SubCommand::Change(change) => change.run().await,
        SubCommand::Channel(channel) => channel.run().await,
        SubCommand::Protocol(protocol) => protocol.run().await,
        #[cfg(feature = "git")]
        SubCommand::Git(git) => git.run().await,
        SubCommand::Move(move_cmd) => move_cmd.run().await,
        SubCommand::List(list) => list.run().await,
        SubCommand::Add(add) => add.run().await,
        SubCommand::Remove(remove) => remove.run().await,
        SubCommand::Reset(reset) => reset.run().await,
        // #[cfg(debug_assertions)]
        SubCommand::Debug(debug) => debug.run().await,
        SubCommand::Fork(fork) => fork.run().await,
        SubCommand::Unrecord(unrecord) => unrecord.run().await,
        SubCommand::Apply(apply) => apply.run().await,
        SubCommand::Remote(remote) => remote.run().await,
        SubCommand::Archive(archive) => archive.run().await,
        SubCommand::Credit(credit) => credit.run().await,
        SubCommand::Tag(tag) => tag.run().await,
        SubCommand::ExternalSubcommand(command) => Ok(run_external_command(command)?),
    }
}

pub fn current_dir() -> Result<PathBuf, anyhow::Error> {
    if let Ok(cur) = std::env::current_dir() {
        Ok(cur)
    } else {
        bail!("Cannot access working directory")
    }
}
