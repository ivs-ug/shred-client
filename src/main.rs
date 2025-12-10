use color_eyre::Result;
use shred_client::subscribe;
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .init();

    info!("Starting ShredStream client...");

    let endpoint = std::env::var("SHREDS_ENDPOINT").expect("SHREDS_ENDPOINT must be set");

    // Subscribe to transactions with buffer size of 5000
    let mut rx = subscribe(endpoint, 5000).await?;

    let mut tx_count = 0;
    let start = std::time::Instant::now();

    info!("Receiving transactions...");

    while let Some((_slot, tx)) = rx.recv().await {
        tx_count += 1;

        // Print transaction signature
        let sig = tx.signatures[0];
        info!("TX #{}: {}", tx_count, sig);

        // Optional: print some stats every 100 transactions
        if tx_count % 100 == 0 {
            let elapsed = start.elapsed();
            let tps = tx_count as f64 / elapsed.as_secs_f64();
            info!(
                "Processed {} transactions in {:.2}s ({:.2} tx/s)",
                tx_count,
                elapsed.as_secs_f64(),
                tps
            );
        }

        // Optional: print transaction details
        info!(
            "  Message type: {}",
            match &tx.message {
                solana_message::VersionedMessage::Legacy(_) => "Legacy",
                solana_message::VersionedMessage::V0(_) => "V0",
            }
        );
        info!("  Signatures: {}", tx.signatures.len());
        info!(
            "  Static accounts: {}",
            tx.message.static_account_keys().len()
        );
        info!("  Instructions: {}", tx.message.instructions().len());

        if let Some(alts) = tx.message.address_table_lookups() {
            info!("  Address table lookups: {}", alts.len());
            for (i, alt) in alts.iter().enumerate() {
                info!(
                    "    ALT #{}: {} (w:{}, r:{})",
                    i,
                    alt.account_key,
                    alt.writable_indexes.len(),
                    alt.readonly_indexes.len()
                );
            }
        }
    }

    info!("ShredStream channel closed");
    Ok(())
}
