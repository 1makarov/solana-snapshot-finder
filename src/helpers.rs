use std::time::{Duration, Instant};

use solana_rpc_client_types::response::RpcSnapshotSlotInfo;
use tokio_stream::StreamExt;

use crate::types::{FullSnapshotName, IncrementalSnapshotName};

#[derive(Debug)]
pub struct SnapshotProvider {
    pub rpc_host: String,
    // snapshot
    pub full_raw: String,
    pub full_name: String,
    pub full_slot: u64,
    pub full_hash: String,
    // incremental snapshot
    pub incremental_raw: String,
    pub incremental_name: String,
    pub incremental_from: u64,
    pub incremental_to: u64,
    pub incremental_hash: String,
    // ext
    pub avg_latency: Duration,
    pub download_speed_mbps: f64,
}

pub async fn get_snapshot_provider(rpc_host: String) -> anyhow::Result<SnapshotProvider> {
    let full_url = format!("http://{}/snapshot.tar.bz2", rpc_host);
    let incremental_url = format!("http://{}/incremental-snapshot.tar.bz2", rpc_host);

    let client = reqwest::Client::new();

    let start = std::time::Instant::now();
    let full_resp = client
        .head(&full_url)
        .timeout(Duration::from_secs(1))
        .send()
        .await?
        .error_for_status()?;
    let latency1 = start.elapsed();

    let start = std::time::Instant::now();
    let incremental_resp = client
        .head(&incremental_url)
        .timeout(Duration::from_secs(1))
        .send()
        .await?
        .error_for_status()?;
    let latency2 = start.elapsed();

    let full_url = full_resp.url().to_string();
    let full_snapshot = FullSnapshotName::from_url(&full_url)
        .map_err(|e| anyhow::anyhow!("parsing snapshot url: {}", e))?;

    let incremental_url = incremental_resp.url().to_string();
    let incremental_snapshot = IncrementalSnapshotName::from_url(&incremental_url)
        .map_err(|e| anyhow::anyhow!("parsing incremental snapshot url: {}", e))?;

    let download_mbps = measure_download_speed(&client, &full_url, Duration::from_secs(5)).await?;

    Ok(SnapshotProvider {
        rpc_host,
        full_raw: full_url,
        full_slot: full_snapshot.slot,
        full_hash: full_snapshot.hash,
        full_name: full_snapshot.filename,
        incremental_raw: incremental_url,
        incremental_from: incremental_snapshot.from_slot,
        incremental_to: incremental_snapshot.to_slot,
        incremental_hash: incremental_snapshot.hash,
        incremental_name: incremental_snapshot.filename,
        avg_latency: latency1.max(latency2),
        download_speed_mbps: download_mbps,
    })
}

async fn measure_download_speed(
    client: &reqwest::Client,
    url: &str,
    duration: Duration,
) -> anyhow::Result<f64> {
    let resp = client.get(url).send().await?.error_for_status()?;

    let mut stream = resp.bytes_stream();

    let start = Instant::now();
    let mut downloaded_bytes = 0u64;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        downloaded_bytes += chunk.len() as u64;

        if start.elapsed() >= duration {
            break;
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    Ok(downloaded_bytes as f64 / 1024.0 / 1024.0 / elapsed)
}

pub fn filter_snapshot_providers(
    mut providers: Vec<SnapshotProvider>,
    max_latency: Duration,
    snapshot_slot: RpcSnapshotSlotInfo,
) -> Option<Vec<SnapshotProvider>> {
    // 1. Фильтр по задержке
    providers.retain(|res| res.avg_latency <= max_latency);

    // 2. Фильтр по full_slot
    providers.retain(|res| res.full_slot == snapshot_slot.full);

    // 3. Фильтр full == incremental_from
    providers.retain(|res| res.full_slot == res.incremental_from);

    // 4. Найти максимальный incremental_to
    let best_incremental_to = providers.iter().map(|res| res.incremental_to).max()?;

    // 5. Оставить провайдеров с лучшим incremental_to
    providers.retain(|res| res.incremental_to == best_incremental_to);

    // 6. Среди них — сортировка по латентности
    providers.sort_by_key(|res| res.avg_latency);

    // 7. Сортируем по скорости скачивания 
    providers.sort_by(|a, b| {
        b.download_speed_mbps
            .partial_cmp(&a.download_speed_mbps)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    Some(providers)
}
