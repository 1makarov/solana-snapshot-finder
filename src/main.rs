use std::{path::PathBuf, time::Duration};

use dialoguer::{Select, theme::ColorfulTheme};
use futures::{StreamExt, TryStreamExt, stream};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use solana_rpc_client::nonblocking::rpc_client::RpcClient;

use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
};

use crate::helpers::{SnapshotProvider, filter_snapshot_providers, get_snapshot_provider};

mod helpers;
mod types;

const MAX_LATENCY: Duration = Duration::from_millis(75);
const ITEMS_TO_SHOW: usize = 10;
const SCAN_THREADS: usize = 64;

#[tokio::main]
async fn main() {
    let client = RpcClient::new("https://api.mainnet-beta.solana.com".to_owned());

    let nodes = client.get_cluster_nodes().await.unwrap();
    let high_snapshot_slot = client.get_highest_snapshot_slot().await.unwrap();
    println!("Highest snapshot slot: {:?}", high_snapshot_slot);
    println!("Total nodes: {}", nodes.len());
    println!(
        "Total nodes with rpc: {}",
        nodes.iter().filter(|node| node.rpc.is_some()).count()
    );

    let rpcs = nodes
        .iter()
        .filter_map(|node| node.rpc.as_ref())
        .map(|rpc| rpc.to_string())
        .collect::<Vec<_>>();

    let validated_providers = get_validated_snapshot_providers(rpcs).await;
    println!(
        "Count of validated snapshot providers: {}",
        validated_providers.len()
    );

    let filtered = filter_snapshot_providers(validated_providers, MAX_LATENCY, high_snapshot_slot)
        .expect("no valid snapshot providers found");

    let items: Vec<String> = filtered
        .iter()
        .take(ITEMS_TO_SHOW)
        .map(|p| format!(" provider: {}, latency: {:?}, full slot: {}, incremental slot: {}, download speed: {:.2} MB/s", p.rpc_host, p.avg_latency, p.full_slot, p.incremental_to, p.download_speed_mbps))
        .collect();

    let selected = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Choose a snapshot provider to download from")
        .items(&items)
        .interact()
        .unwrap();

    let best = &filtered[selected];
    let client = reqwest::Client::new();

    println!("Downloading snapshot from: {}", best.full_raw);
    let multi = MultiProgress::new();
    let full_fut = download_file(
        client.clone(),
        best.full_raw.clone(),
        best.full_name.clone().into(),
        Some(multi.clone()),
    );

    println!(
        "Downloading incremental snapshot from: {}",
        best.incremental_raw
    );
    let incremental_fut = download_file(
        client.clone(),
        best.incremental_raw.clone(),
        best.incremental_name.clone().into(),
        Some(multi.clone()),
    );

    tokio::try_join!(full_fut, incremental_fut).expect("failed to download snapshots");
}

pub async fn get_validated_snapshot_providers(rpcs: Vec<String>) -> Vec<SnapshotProvider> {
    let pb = ProgressBar::new(rpcs.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} validating rpcs [{bar:40.cyan/blue}] {pos}/{len} ({per_sec})",
        )
        .expect("progress style")
        .progress_chars("=> "),
    );

    let validated = stream::iter(rpcs)
        .map(|rpc| {
            let pb = pb.clone();
            async move {
                let res = get_snapshot_provider(rpc).await;
                pb.inc(1);
                res.ok()
            }
        })
        .buffer_unordered(SCAN_THREADS) // чуть выше параллелизм
        .filter_map(async |v| v)
        .collect()
        .await;

    pb.finish_with_message("Validation completed");

    validated
}

pub async fn download_file(
    client: reqwest::Client,
    url: String,
    path: PathBuf,
    multi_pb: Option<MultiProgress>,
) -> anyhow::Result<()> {
    const BUFFER_SIZE: usize = 128 * 1024 * 1024; // 128 MB

    let resp = client.get(&url).send().await?;
    if !resp.status().is_success() {
        anyhow::bail!("failed to download file: HTTP {}", resp.status());
    }

    let total_bytes = resp.content_length().unwrap_or(0);
    let pb = match multi_pb {
        Some(mpb) => mpb.add(ProgressBar::new(total_bytes)),
        None => ProgressBar::new(total_bytes),
    };

    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} {bytes}/{total_bytes} [{bar:40.cyan/blue}] {bytes_per_sec} {eta}",
        )?
        .progress_chars("#>-"),
    );

    let file = File::create(path).await?;
    let mut writer = BufWriter::with_capacity(BUFFER_SIZE, file);

    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.try_next().await? {
        writer.write_all(&chunk).await?;
        pb.inc(chunk.len() as u64);
    }

    writer.flush().await?;
    pb.finish_with_message("Download completed");

    Ok(())
}
