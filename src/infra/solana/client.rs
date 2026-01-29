// Responsible for all communication with the Solana blockchain.

use primitive_types::H256;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::{
    instruction::{AccountMeta, Instruction},
    pubkey::Pubkey,
};
use solana_sdk::{
    commitment_config::CommitmentConfig,
    signer::{keypair::read_keypair_file, Signer},
    transaction::Transaction,
};
use std::str::FromStr;

use crate::infra::config;

// Define the structure of the on-chain account that stores the Merkle root.
// This must match the struct in the smart contract.
#[allow(dead_code)] // Reserved for future use (e.g., reading account data)
pub struct MerkleRootAccount {
    pub root: [u8; 32],
    pub timestamp: i64,
}

// Helper function to get the RPC client and payer keypair.
async fn get_client_and_payer() -> anyhow::Result<(RpcClient, solana_sdk::signer::keypair::Keypair)>
{
    let rpc_url = config::solana_rpc_url();
    let payer = read_keypair_file(&*shellexpand::tilde("~/.config/solana/id.json"))
        .map_err(|e| anyhow::anyhow!("Failed to read keypair file: {}", e))?;

    let client = RpcClient::new_with_commitment(rpc_url, CommitmentConfig::confirmed());
    Ok((client, payer))
}

// We need a predictable address for our Merkle root account using a PDA.
fn get_merkle_root_account_pubkey() -> anyhow::Result<(Pubkey, u8)> {
    let program_id = Pubkey::from_str(&config::solana_program_id())?;
    let seeds = b"merkle_root_account";
    let (pda, bump) = Pubkey::find_program_address(&[seeds], &program_id);
    Ok((pda, bump))
}

/// Initializes the on-chain Merkle root account.
/// This only needs to be called once.
pub async fn initialize() -> anyhow::Result<()> {
    let (client, payer) = get_client_and_payer().await?;
    let (merkle_root_account_pubkey, _bump) = get_merkle_root_account_pubkey()?;
    let program_id = Pubkey::from_str(&config::solana_program_id())?;

    // Check if the account already exists.
    if client.get_account(&merkle_root_account_pubkey).await.is_ok() {
        println!("Merkle root account already initialized.");
        return Ok(());
    }

    println!("Initializing Merkle root account...");
    let initial_root = H256::zero();

    // Build the instruction manually
    // Discriminator for initialize: [175, 175, 109, 31, 13, 152, 155, 237]
    let accounts = vec![
        AccountMeta::new(merkle_root_account_pubkey, false),
        AccountMeta::new(payer.pubkey(), true),
        AccountMeta::new_readonly(solana_program::system_program::ID, false),
    ];

    let mut instruction_data = vec![175, 175, 109, 31, 13, 152, 155, 237]; // initialize discriminator
    instruction_data.extend_from_slice(&initial_root.to_fixed_bytes());

    let instruction = Instruction {
        program_id,
        accounts,
        data: instruction_data,
    };

    let mut transaction = Transaction::new_with_payer(&[instruction], Some(&payer.pubkey()));

    let recent_blockhash = client.get_latest_blockhash().await?;
    transaction.sign(&[&payer], recent_blockhash);
    client.send_and_confirm_transaction(&transaction).await?;

    println!("Successfully initialized Merkle root account on-chain.");
    Ok(())
}

/// Reads the trusted Merkle root from the Solana blockchain.
pub async fn read_root() -> anyhow::Result<H256> {
    let (client, _payer) = get_client_and_payer().await?;
    let (merkle_root_account_pubkey, _bump) = get_merkle_root_account_pubkey()?;

    let account_info = client.get_account(&merkle_root_account_pubkey).await?;
    let account_data = account_info.data;

    // Account structure: 8-byte discriminator + 32-byte root + 8-byte timestamp
    if account_data.len() < 48 {
        return Err(anyhow::anyhow!("Account data too short"));
    }

    // Skip the 8-byte discriminator and read the 32-byte root
    let mut root_bytes = [0u8; 32];
    root_bytes.copy_from_slice(&account_data[8..40]);
    Ok(H256::from(root_bytes))
}

/// Writes a new Merkle root to the Solana blockchain.
pub async fn write_root(new_root: H256) -> anyhow::Result<()> {
    let (client, payer) = get_client_and_payer().await?;
    let (merkle_root_account_pubkey, _bump) = get_merkle_root_account_pubkey()?;
    let program_id = Pubkey::from_str(&config::solana_program_id())?;

    // Build the instruction manually
    // Discriminator for update_root: [58, 195, 57, 246, 116, 198, 170, 138]
    let accounts = vec![
        AccountMeta::new(merkle_root_account_pubkey, false),
        AccountMeta::new(payer.pubkey(), true),
    ];

    let mut instruction_data = vec![58, 195, 57, 246, 116, 198, 170, 138]; // update_root discriminator
    instruction_data.extend_from_slice(&new_root.to_fixed_bytes());

    let instruction = Instruction {
        program_id,
        accounts,
        data: instruction_data,
    };

    let mut transaction = Transaction::new_with_payer(&[instruction], Some(&payer.pubkey()));
    let recent_blockhash = client.get_latest_blockhash().await?;
    transaction.sign(&[&payer], recent_blockhash);
    let signature = client.send_and_confirm_transaction(&transaction).await?;

    println!(
        "Successfully wrote new root to the Solana blockchain: {}",
        hex::encode(new_root.as_bytes())
    );
    println!(
        "Transaction Signature: https://explorer.solana.com/tx/{}?cluster=devnet",
        signature
    );

    Ok(())
}

