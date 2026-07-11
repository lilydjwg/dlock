use std::fs;
use std::time::Duration;

use clap::Parser;
use etcd_client as etcd;
use eyre::{Result, eyre};
use serde::Deserialize;

use lilysetup::*;

#[derive(Deserialize)]
struct Config {
  ttl: Option<u32>,
  lockname: String,
  endpoints: Vec<String>,
  cert: Option<EtcdCert>,
}

#[derive(Deserialize)]
struct EtcdCert {
  cert: String,
  key: String,
  ca: String,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
  #[arg(long)]
  config: String,
  #[arg(long)]
  nodename: String,
  #[arg(required = true, help="command to run holding the lock")]
  cmd: Vec<String>,
}

#[tokio::main(flavor="current_thread")]
async fn main() -> Result<()> {
  let args = Args::parse();

  setup_logging("info")?;

  let config = fs::read_to_string(&args.config)?;
  let config: Config = toml::from_str(&config)?;

  let options = if let Some(c) = config.cert {
    let cert = fs::read(c.cert)?;
    let key = fs::read(c.key)?;
    let cacert = fs::read(c.ca)?;
    let ident = etcd::Identity::from_pem(cert, key);
    let cacert = etcd::Certificate::from_pem(cacert);

    let tls = etcd::TlsOptions::new()
      .ca_certificate(cacert)
      .identity(ident);

    Some(etcd::ConnectOptions::new().with_tls(tls))
  } else {
    None
  };

  let mut client = match etcd::Client::connect(
    &config.endpoints, options,
  ).await {
    Ok(c) => c,
    Err(e) => {
      error!("Error: {:?}", e);
      return Err(e.into());
    },
  };

  let lease_res = client.lease_grant(i64::from(config.ttl.unwrap_or(5)), None).await?;
  let lease = lease_res.id();
  let keeper = tokio::spawn(lease_keeper(client.lease_client(), lease_res));

  let mut res = client.campaign(config.lockname, args.nodename, lease).await?;
  let leaderkey = res.take_leader().unwrap();
  info!("Running {}...", args.cmd.join(" "));

  let mut child = tokio::process::Command::new(&args.cmd[0])
    .args(&args.cmd[1..])
    .kill_on_drop(true)
    .spawn()?;

  tokio::select! {
    e = keeper => {
      error!("leadership gone: {}", e.unwrap_err());
      if let Some(pid) = child.id() {
        unsafe {
          libc::kill(pid.try_into().unwrap(), libc::SIGTERM);
        }
        let _ = tokio::time::timeout(Duration::from_secs(1), child.wait()).await;
      }
      drop(child);
    },
    st = child.wait() => {
      info!("child process exited with {}", st?);
      let options = etcd::ResignOptions::new()
        .with_leader(leaderkey);
      client.resign(Some(options)).await?;
    }
  }


  Ok(())
}

async fn lease_keeper(
  mut client: etcd::LeaseClient,
  lease_res: etcd::LeaseGrantResponse,
) -> Result<()> {
  let lease = lease_res.id();
  let mut ttl = lease_res.ttl();

  let (mut keeper, mut stream) = client.keep_alive(lease).await?;
  loop {
    let sleep_dur = Duration::from_millis((ttl * 1000 / 2) as u64);
    tokio::select! {
      _ = tokio::time::sleep(sleep_dur) => {
        debug!("Keep alive");
        keeper.keep_alive().await?;
      },
      r = stream.message() => {
        debug!("Received message: {:?}", r);
        if let Some(res) = r? {
          ttl = res.ttl();
          if ttl == 0 {
            return Err(eyre!("lease expired."))
          }
        }
      }
    }
  }
}
