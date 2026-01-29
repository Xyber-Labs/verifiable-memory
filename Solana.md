# Solana Integration for Verifiable Database

This document outlines the steps taken to set up a Solana smart contract (program) for storing and managing the Merkle root of the verifiable database on-chain.

## Part 1: Setting Up the On-Chain Program

This part covers the creation, configuration, and deployment of the Solana smart contract.

### 1. Install Solana & Anchor Tooling

A specific development environment is required to build for Solana. We installed:

- **Agave (Solana CLI):** The core toolchain for compiling Solana programs and interacting with the network. Agave is the new name for the official Solana validator client.
  ```bash
  sh -c "$(curl -sSfL https://release.anza.xyz/stable/install)"
  ```

- **Anchor Framework:** A framework that dramatically simplifies writing secure Solana programs in Rust.
  ```bash
  cargo install --git https://github.com/coral-xyz/anchor avm --force
  avm install latest
  avm use latest
  ```

### 2. Create the Anchor Project

We created a new project for our on-chain program.

```bash
anchor init solana_program
```

### 3. Implement the Smart Contract

The core logic was written in `solana_program/programs/verifiable_db_program/src/lib.rs`.

**Key Components:**

- **`MerkleRootAccount`**: An on-chain account structure to store the 32-byte Merkle root and a timestamp.
  ```rust
  #[account]
  pub struct MerkleRootAccount {
      pub root: [u8; 32],
      pub timestamp: i64,
  }
  ```

- **`initialize` instruction**: A one-time function to create the `MerkleRootAccount` on the blockchain.

- **`update_root` instruction**: The main "write" function that allows an authorized user to update the Merkle root stored on-chain.

### 4. Build & Deploy

This was a multi-step process to get the program running on the Solana devnet.

**a. Configure Solana CLI for Devnet**

Set your Solana CLI to use devnet:

```bash
solana config set --url https://api.devnet.solana.com
```

**b. Create a Wallet (if needed)**

Every transaction (including deployment) requires a signer and a fee payer. If you don't have a wallet, create one:

```bash
solana-keygen new -o ~/.config/solana/id.json --no-bip39-passphrase
```

**c. Fund the Wallet with Devnet SOL**

Request free devnet SOL from the devnet faucet to pay for transaction fees.

```bash
solana airdrop 2
```

**d. Build and Deploy the Program**

The `anchor deploy` command compiles the Rust code and uploads it to the devnet.

```bash
cd solana_program
anchor deploy
```
During the first deployment, the network assigned our program a unique address (Program ID). We had to update `lib.rs` with this new ID and redeploy to fix a `DeclaredProgramIdMismatch` error. The final, correct Program ID is: `6fSQZwqdsr8zVSbE8DTo4tsHDW4af3iZyB5KGzEGqyW8`.

**Result:** The smart contract is now live and running on the Solana devnet.

## Part 2: Client-Side Integration

This part covers connecting the main `verifiable_db` application to our deployed on-chain program.

1.  **Add Dependencies**: The `solana-sdk`, `solana-client`, and `anchor-client` crates were added to the main project's `Cargo.toml`.

2.  **Create the Solana client module**: The client-side logic for interacting with the Solana program lives in `src/infra/solana/client.rs`. It defines the client-side representation of our on-chain account and contains functions to call the program's instructions.

3.  **Implement Client Logic**: The following functions were implemented in `src/infra/solana/client.rs`:
    *   `initialize()`: An async function that creates the on-chain Merkle root account if it doesn't already exist.
    *   `read_root()`: An async function that fetches and returns the current Merkle root from the blockchain.
    *   `write_root(new_root)`: An async function that sends a transaction to the program to update the on-chain Merkle root.

4.  **Integrate with `main.rs`**: `main.rs` calls into the library's Solana client (`verifiable_memory_example::solana`), which is implemented in `src/infra/solana/client.rs`. The application now fully interacts with the Solana devnet for storing and retrieving the trust anchor.

## Next Steps

The integration is complete. The application can now be run to test the end-to-end flow, from updating the database to committing the state change on the Solana devnet and verifying data against the on-chain root.
