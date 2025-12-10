pub mod shredstream {
    include!("shredstream.rs");
}

use color_eyre::eyre::Result;
use shredstream::{shredstream_proxy_client::ShredstreamProxyClient, SubscribeEntriesRequest};
use solana_transaction::versioned::VersionedTransaction;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

#[derive(Debug, serde::Deserialize)]
pub struct Entry {
    pub num_hashes: u64,
    pub hash: solana_hash::Hash,
    pub transactions: Vec<VersionedTransaction>,
}

/// Convenient wrapper to subscribe to shredstream and receive transactions via channel
///
/// # Arguments
/// * `url` - ShredStream gRPC endpoint (e.g., "http://84.32.64.135:9999")
/// * `buffer_size` - Channel buffer size (recommended: 1000-10000)
///
/// # Returns
/// Receiver channel that yields `VersionedTransaction`
///
/// # Example
/// ```no_run
/// use shred_client::subscribe;
///
/// #[tokio::main]
/// async fn main() {
///     let mut rx = subscribe("http://127.0.0.1:9999", 5000).await.unwrap();
///     
///     while let Some(tx) = rx.recv().await {
///         println!("Received tx: {}", tx.signatures[0]);
///     }
/// }
/// ```
pub async fn subscribe(
    url: impl AsRef<str>,
    buffer_size: usize,
) -> Result<mpsc::Receiver<(u64, VersionedTransaction)>> {
    let url = url.as_ref().to_string();
    let (tx, rx) = mpsc::channel(buffer_size);

    tokio::spawn(async move {
        if let Err(e) = subscribe_loop(url, tx).await {
            error!("ShredStream subscription failed: {:#?}", e);
        }
    });

    Ok(rx)
}

async fn subscribe_loop(
    url: String,
    tx: mpsc::Sender<(u64, VersionedTransaction)>,
) -> Result<()> {
    loop {
        info!("Connecting to ShredStream at {}", url);

        let mut client = match ShredstreamProxyClient::connect(url.clone()).await {
            Ok(c) => c,
            Err(e) => {
                error!("Connection failed: {:#?}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let mut stream = match client.subscribe_entries(SubscribeEntriesRequest {}).await {
            Ok(s) => s.into_inner(),
            Err(e) => {
                error!("Subscription failed: {:#?}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        info!("Successfully subscribed to ShredStream");

        while let Some(slot_entry_result) = stream.message().await.transpose() {
            match slot_entry_result {
                Ok(slot_entry) => {
                    let entries = match bincode::deserialize::<Vec<Entry>>(&slot_entry.entries) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!("Entry deserialization failed: {:#?}", e);
                            continue;
                        }
                    };

                    let tx_count = entries.iter().map(|e| e.transactions.len()).sum::<usize>();
                    if tx_count > 0 {
                        info!(
                            "Slot {}: {} entries, {} transactions",
                            slot_entry.slot,
                            entries.len(),
                            tx_count
                        );
                    }

                    // Stream all transactions to channel
                    for entry in entries {
                        for transaction in entry.transactions {
                            if tx.send((slot_entry.slot, transaction)).await.is_err() {
                                // Receiver dropped, exit gracefully
                                info!("Receiver dropped, stopping ShredStream subscription");
                                return Ok(());
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Stream error: {:#?}", e);
                    break;
                }
            }
        }

        warn!("Stream ended, reconnecting...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}

/// Subscribe and yield raw entries instead of individual transactions
///
/// Useful if you need to process entries in batches or need slot information
pub async fn subscribe_entries(
    url: impl AsRef<str>,
    buffer_size: usize,
) -> Result<mpsc::Receiver<(u64, Vec<Entry>)>> {
    let url = url.as_ref().to_string();
    let (tx, rx) = mpsc::channel(buffer_size);

    tokio::spawn(async move {
        if let Err(e) = subscribe_entries_loop(url, tx).await {
            error!("ShredStream entries subscription failed: {:#?}", e);
        }
    });

    Ok(rx)
}

async fn subscribe_entries_loop(
    url: String,
    tx: mpsc::Sender<(u64, Vec<Entry>)>,
) -> Result<()> {
    loop {
        info!("Connecting to ShredStream at {}", url);

        let mut client = match ShredstreamProxyClient::connect(url.clone()).await {
            Ok(c) => c,
            Err(e) => {
                error!("Connection failed: {:#?}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        let mut stream = match client.subscribe_entries(SubscribeEntriesRequest {}).await {
            Ok(s) => s.into_inner(),
            Err(e) => {
                error!("Subscription failed: {:#?}", e);
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                continue;
            }
        };

        info!("Successfully subscribed to ShredStream (entries mode)");

        while let Some(slot_entry_result) = stream.message().await.transpose() {
            match slot_entry_result {
                Ok(slot_entry) => {
                    let entries = match bincode::deserialize::<Vec<Entry>>(&slot_entry.entries) {
                        Ok(e) => e,
                        Err(e) => {
                            warn!("Entry deserialization failed: {:#?}", e);
                            continue;
                        }
                    };

                    if tx.send((slot_entry.slot, entries)).await.is_err() {
                        info!("Receiver dropped, stopping ShredStream subscription");
                        return Ok(());
                    }
                }
                Err(e) => {
                    error!("Stream error: {:#?}", e);
                    break;
                }
            }
        }

        warn!("Stream ended, reconnecting...");
        tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
    }
}