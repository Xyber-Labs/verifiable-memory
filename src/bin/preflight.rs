use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::signer::keypair::read_keypair_file;
use solana_sdk::signer::Signer;
use std::str::FromStr;

use verifiable_memory_example::infra::config;
use verifiable_memory_example::infra::solana;

fn usage_and_exit() -> ! {
    eprintln!(
        "Usage: cargo run --bin preflight -- [--init-pda-if-missing]\n\
         \n\
         Requires env vars:\n\
           DATABASE_URL, SOLANA_RPC_URL, SOLANA_PROGRAM_ID, BATCH_COMMIT_SIZE\n\
         And Solana payer key:\n\
           ~/.config/solana/id.json\n"
    );
    std::process::exit(2);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        usage_and_exit();
    }

    let init_pda_if_missing = args.iter().any(|a| a == "--init-pda-if-missing");

    // Force-read config (nice error messages if missing)
    let rpc_url = config::solana_rpc_url();
    let program_id_str = config::solana_program_id();
    let _ = config::database_url();
    let batch = config::batch_commit_size();

    println!("> Preflight:");
    println!("  SOLANA_RPC_URL={}", rpc_url);
    println!("  SOLANA_PROGRAM_ID={}", program_id_str);
    println!("  BATCH_COMMIT_SIZE={}", batch);

    // Same payer location the service uses.
    let payer_path = shellexpand::tilde("~/.config/solana/id.json").to_string();
    let payer =
        read_keypair_file(&payer_path).map_err(|e| anyhow::anyhow!("Failed to read {}: {}", payer_path, e))?;

    let client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());

    // Basic RPC connectivity
    let version = client.get_version().await?;
    println!("  RPC version: {}", version.solana_core);

    // Payer balance
    let balance_lamports = client.get_balance(&payer.pubkey()).await?;
    let sol = balance_lamports as f64 / 1_000_000_000_f64;
    println!("  Payer: {}", payer.pubkey());
    println!("  Payer balance: {} lamports (~{:.6} SOL)", balance_lamports, sol);
    if balance_lamports < 10_000_000 {
        eprintln!("  Warning: payer balance looks low; devnet transactions may fail.");
    }

    // Program account existence
    let program_id = Pubkey::from_str(&program_id_str)
        .map_err(|e| anyhow::anyhow!("SOLANA_PROGRAM_ID is not a valid pubkey: {}", e))?;
    let program_acct = client
        .get_account(&program_id)
        .await
        .map_err(|e| anyhow::anyhow!("Program account not found on cluster: {} ({})", program_id, e))?;
    if !program_acct.executable {
        eprintln!("  Warning: program account exists but is not marked executable.");
    } else {
        println!("  Program account is deployed + executable.");
    }

    // PDA existence
    let (pda, _bump) = Pubkey::find_program_address(&[b"merkle_root_account"], &program_id);
    println!("  Merkle root PDA: {}", pda);

    let pda_exists = client.get_account(&pda).await.is_ok();
    if pda_exists {
        println!("  PDA account exists.");
    } else if init_pda_if_missing {
        println!("  PDA missing -> initializing on-chain merkle root account...");
        solana::initialize().await?;
        // Recheck
        client
            .get_account(&pda)
            .await
            .map_err(|e| anyhow::anyhow!("PDA still missing after initialize: {}", e))?;
        println!("  PDA initialized successfully.");
    } else {
        return Err(anyhow::anyhow!(
            "Merkle root PDA does not exist. Re-run with --init-pda-if-missing"
        ));
    }

    // Root readable
    let root = solana::read_root().await?;
    println!("  Root is readable from chain (ok). Root bytes[0..4]={:02x?}", &root.as_bytes()[0..4]);

    println!("> Preflight OK.");
    Ok(())
}

