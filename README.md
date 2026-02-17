# The proposed architecture:

![Verifiable Database Architecture](docs/images/architecture.jpg)

## Current architecture for integration with TDX and SGX

![Current integration architecture](docs/images/integration.jpeg)

## Future architecture for integration with TDX and SGX

![Future integration architecture](docs/images/futureintegration.jpeg)

## Motivation: Verifiable Off-Chain State

## The Problem

A Trusted Execution Environment (TEE) provides a secure enclave for executing code, but it cannot host a database internally. This creates a challenge: how can an application running inside a TEE (like an AI agent) interact with a database that resides in the untrusted outside world, while ensuring the data has not been tampered with?

This Proof of Concept explores a solution to this problem by comparing two primary architectural approaches

---

## Option 1: Full State Decentralization (cNFT / IPFS Approach)

This approach treats the agent's entire memory or database state as decentralized content. The data is stored on a network like IPFS, and its integrity is anchored to the blockchain using a Merkle tree, similar to Solana's compressed NFT (cNFT) standard.

*   **How it works:**
    *   **Content:** Stored on IPFS.
    *   **Ownership & State:** Managed in an off-chain Merkle tree.
    *   **Proof of Integrity:** The Merkle root of the tree is stored on the Solana blockchain.

*   **Pros:**
    *   **Full Decentralization:** The entire state is distributed and not reliant on a single database server.
    *   **Data Recoverability:** Since all data is on a public network, the state can be fully reconstructed from the chain and IPFS, even if the primary service is lost.

*   **Cons:**
    *   **High Latency:** Every state change requires interacting with IPFS and recalculating the tree, which is significantly slower than a traditional database write.
    *   **Higher Cost:** While cheaper than storing all data on-chain, frequent updates can still incur notable costs.
    *   **Inconvenience:** Not well-suited for applications requiring frequent, low-latency reads and writes, complex queries, or relational data.

---

## Option 2: Verifiable Database (The Implemented Solution)

This approach uses a high-performance, traditional database (like PostgreSQL) for storing data and leverages a Sparse Merkle Tree (SMT) to anchor the integrity of that data on-chain.

*   **How it works:**
    *   **Content:** Stored in a standard PostgreSQL database.
    *   **Proof of Integrity:** A single cryptographic hash (the SMT root) representing the entire database state is stored on the Solana blockchain.

*   **Pros:**
    *   **High Performance:** Achieves the speed and querying capabilities of a traditional database. Writes and reads are extremely fast.
    *   **Low Cost:** On-chain transactions are only needed to update a single hash, and with batching, this cost is minimized across many thousands of database operations.
    *   **Convenience:** Developers can use standard SQL and database tools.

*   **Cons:**
    *   **Centralized Data Storage:** The data itself is not decentralized. If the database is destroyed, the data is lost.
    *   **No On-Chain Recoverability:** The blockchain only guarantees the *integrity* of the data; it cannot be used to *restore* the data.

---

## Conclusion: Why This PoC Uses the Verifiable Database Approach

For the target use case of AI agents, the verifiable database model was chosen as the superior architecture. The critical requirement is to **prevent and detect tampering** of the agent's state, not necessarily to ensure the state is fully decentralized or recoverable from the blockchain.

The agents' operational integrity depends on knowing their memory is correct, but if the memory is lost entirely, it is not a critical failure for the overall system. Therefore, the immense performance, cost, and convenience benefits of the verifiable database approach far outweigh the advantage of full data decentralization offered by the cNFT model for this specific application.

# Verifiable Database PoC

This project is a Proof of Concept for a verifiable database system that uses a Sparse Merkle Tree (SMT) to create cryptographic commitments to its state. These commitments (Merkle roots) are anchored on the Solana blockchain, creating a tamper-evident audit trail.

The architecture ensures that a TEE (Trusted Execution Environment) can interact with a database, verify the integrity and authenticity of the data it receives, and prove to any third party that the data is correct and hasn't been tampered with.

## Architecture Overview

```text
+-----------------------------------------------------------------------------+
|                                TEE ENCLAVE                                  |
|                                                                             |
|  +------------------+          (Internal Call)        +------------------+  |
|  |                  |                POST             |                  |  |
|  |  Client Program  |-------------------------------->| Verifiable DB    |  |
|  |  (TEE agent)     |                                 |       API        |  |
|  |                  |                                 |                  |  |
|  +------------------+                                 +--------+---------+  |
|                                                                 |      ^    |
+-----------------------------------------------------------------|------|----+
                                                                  |      |
                                                            (RPC) |      | (SQL)
                                                                  v      | R/W SMT,DATA
                                                        +--------------+ |
                                                        |  Blockchain  | |
                                                        |(R/W SMT ROOT)| |
                                                        +--------------+ v
                                                                  +------------+
                                                                  |  Database  |
                                                                  +------------+
```

The system is composed of two main pieces:

1. **TEE Agent (external client)**: any application running in a TEE (e.g. an AI agent service) that calls the Verifiable DB HTTP API.  
   In this repo, the “agent flow” is demonstrated via integration tests under `tests/` (e.g. `test_schema_update.rs`) and you can also interact manually via Swagger UI.
2. **Verifiable API Service (runs in TEE)**: the trusted component. It handles DB interactions, proof generation + verification against the trusted root, and commits new roots to Solana.

## Source Layout (Rust module structure)

The crate is organized by responsibility (transport vs domain vs infra). The main executable for manual interaction is `src/bin/api_server.rs` (Swagger UI), and the end-to-end flows live in integration tests under `tests/`.

```text
src/
  lib.rs                       # crate public surface + re-exports

  app/
    database_service.rs         # DB + SMT orchestration (use-cases)

  transport/
    http/
      types.rs                  # HTTP types + AppState
      router.rs                 # Axum router + OpenAPI (ApiDoc)
      handlers/                 # endpoint handlers split by concern

  domain/
    commitment/
      root_manager.rs           # dual-root batching + trusted_state.json
    model/
      mod.rs                    # VerifiableModel trait
      registry.rs               # ModelRegistry
      examples.rs               # UserModel/ProductModel/WidgetModel (PoC models)
    verify/
      verifier.rs               # SMT proof verification helpers

  crypto/
    hashing.rs                  # canonical JSON hashing + domain separation

  storage/
    smt/
      store.rs                  # Sparse Merkle Tree wrapper (in-memory + proof generation)
      postgres.rs               # merkle_nodes persistence

  infra/
    config.rs                   # env parsing (DATABASE_URL, SOLANA_RPC_URL, SOLANA_PROGRAM_ID, BATCH_COMMIT_SIZE)
    solana/
      client.rs                 # Solana RPC client (reads SOLANA_RPC_URL + SOLANA_PROGRAM_ID from env)

  bin/
    api_server.rs               # standalone API server binary (Swagger UI)
```

## Generic / Dynamic Models (Bring Your Own Postgres Schema)

This PoC started with a few hardcoded example models, but the Verifiable DB service is now designed to be **generic**: you can plug in **your own application tables** without modifying Rust code.

### How it works

- **Schema bootstrap endpoint**: you POST a JSON schema spec to `POST /bootstrap/apply-schema`.
  - The service will create the tables in PostgreSQL (DDL is generated by the server, not executed from raw SQL supplied by the client).
  - The schema is persisted into a registry table (`verifiable_models`) and loaded into an in-memory `ModelRegistry` so the API can validate/route requests at runtime.
- **Model registry tables** (created automatically):
  - `verifiable_models`: per-table metadata (table name, PK field/kind, column specs, server-generated CREATE TABLE SQL).
  - `verifiable_registry_meta`: stores a `schema_hash` used for change detection.

### Reset-on-schema-change (single-tenant safety mode)

This repo currently runs in a **single-tenant** mode with a strong safety property: **if the database structure changes, we reset state** to avoid mixing old SMT commitments with a new schema.

On `POST /bootstrap/apply-schema`, the service computes a deterministic `schema_hash` of the provided table specs. If the hash differs from what was previously stored (or `force_reset=true`), it performs a full reset:

- **Drops all managed tables** (both previously registered tables and the tables in the incoming schema spec)
- **Clears SMT persistence** by truncating `merkle_nodes`
- **Zeros BOTH roots**:
  - sets `temporary_root = 0` in memory
  - writes `main_root = 0` to Solana
- **Deletes `trusted_state.json`** (so the trusted local root cannot conflict with the new zeroed chain root)

This makes schema changes explicit and safe: when you change your application tables, the verifiable layer resets cleanly and starts committing from a known zero state.

### DB-generated primary keys (SERIAL / BIGSERIAL)

The generic model layer supports **auto-increment IDs** (Postgres `SERIAL`/`BIGSERIAL`).

- On write (`create-batch`), you **do not** send `id`.
- The service inserts rows and uses `RETURNING id` to fetch the generated PK.
- The API returns the inserted `ids`, which you can then pass back to `read-batch`.

This is important for real apps: most production schemas rely on DB-generated IDs, and the SMT keys are derived from the canonical string form of the primary key.

## High-Throughput Batching: The Dual-Root System

To achieve high performance and reduce on-chain transaction costs, the system uses a dual-root architecture for batching state commitments. Instead of writing to the blockchain on every operation, it groups multiple updates together.

*   **`temporary_root` (In-Memory)**: This root lives inside the TEE API service. It is updated on **every single write operation**, representing the absolute latest state of the database. Write and read operations are extremely fast as they only interact with this in-memory root.

*   **`main_root` (On-Chain)**: This is the globally trusted root stored on the Solana blockchain. It is updated periodically by a background task.

### How Verification Works (CreateBatch Flow)

The key security guarantee is that the Verifiable API Service (running in a TEE) handles the entire verification and commitment process internally.

1.  **Request Initiation**: The `TEE Agent` sends a request to the API to create new data.
2.  **Proposed State Generation**: The `API Service` receives the request. It interacts with the `Database` to:
    *   Insert the new records.
    *   Calculate the **new Merkle root** (`proposed_root`).
    *   Generate a **Merkle proof** linking the current `temporary_root` to the `proposed_root`.
3.  **Fetch Trusted State (In-Memory)**: The `API Service` retrieves the current `temporary_root` from its memory. This is the trusted "before" state for this operation.
4.  **Internal Verification**: The `API Service` verifies that the `proof` correctly links the `temporary_root` to the `proposed_root`.
5.  **Update Temporary State**:
    *   **If valid**, the `API Service` updates its in-memory `temporary_root` to the `proposed_root` and returns a success response to the agent. The write is now complete from the client's perspective.
    *   **If invalid**, an error is returned.
6.  **Asynchronous Commit to Blockchain**: The update increments a counter. When the counter reaches the `BATCH_COMMIT_SIZE` threshold:
    *   A background task pauses new writes to prevent race conditions.
    *   It sends a transaction to `Solana` to write the current `temporary_root` on-chain, making it the new `main_root`.
    *   Writes are resumed.


## Crash Recovery & Trusted State

One of the critical challenges in a TEE environment is handling unexpected shutdowns or crashes. If the system crashes after updating the database but *before* committing the new root to the blockchain, the on-chain state (the "Master Root") will be stale.

To solve this, the system implements a **Trusted Local Storage** mechanism:

1.  **Trusted State File**: The `RootManager` maintains a secure local file (`trusted_state.json`) inside the TEE. This file acts as a persistent, trusted cache for the latest `temporary_root`.
2.  **Atomic Updates**: On every write operation, the new root is written to this trusted file *before* the in-memory state is updated. This ensures that if the system crashes, the latest root is safely persisted.
3.  **Automatic Recovery**:
    *   On startup, the system reads the **Master Root** from Solana and the **Local Root** from the trusted file.
    *   If they differ, the system detects that a crash occurred.
    *   It initializes using the **Local Root** (restoring the latest valid state) and automatically schedules a background commit to sync this state to the blockchain.

This ensures that the TEE can always recover its latest state and verify database integrity, even if the blockchain is lagging behind due to a crash.

## API: Generic Read/Write Endpoints

After bootstrapping schema, you can use the generic per-model endpoints:

- **Healthcheck**: `GET /health`
  - Use this for load balancers / clients to confirm the service is up **and the database is reachable**.
  - Returns:
    - `200` with `{ "success": true, "data": { "status": "ok" } }` when DB ping succeeds
    - `503` with `{ "success": false, "error": "DB ping failed: ..." }` when DB ping fails
  - Example:

```bash
curl -i http://localhost:3000/health
```

- **Write**: `POST /api/models/{model}/create-batch`
- **Upsert (update semantics)**: `POST /api/models/{model}/upsert`
- **Read**: `POST /api/models/{model}/read-batch`
- **Read latest N**: `POST /api/models/{model}/read-latest`
- **Inspect live DB schema**: `GET /bootstrap/schema`
 - **Clear all client data + reset roots**: `POST /bootstrap/clear-data`
  - Returns the **current Postgres schema** (tables/columns/PK) as seen by the database.
  - **Internal service tables are hidden** (e.g. `merkle_nodes`, `verifiable_models`, `verifiable_registry_meta`), so you only see your application tables (e.g. `agents`, `actions`, etc.).

These endpoints work for *any* registered model/table. The service:

- Writes to Postgres
- Updates the SMT
- Verifies the SMT proof transition inside the trusted API
- Updates `temporary_root` (and batches commits to Solana using `BATCH_COMMIT_SIZE`)

### Standard response shape

All read/write endpoints return `ApiResponse` with a consistent `data` object:

```json
{
  "success": true,
  "data": {
    "records": [{ "...": "..." }],
    "ids": ["..."],
    "verified": true,
    "meta": { }
  }
}
```

- `records`: canonical rows as returned by Postgres (`row_to_json(table.*)`).
- `ids`: primary key values for the returned rows.
- `verified`: `true` when the SMT proof verifies against the trusted `temporary_root`.
- `meta`: optional extra info (e.g. `limit`, `committed`, `proposed_root`).

### Read latest N (verified)

To fetch the most recent rows from a table (ordered by primary key descending) while still getting SMT verification, call:

- `POST /api/models/{model}/read-latest`

Request body:

```json
{ "limit": 5 }
```

Notes:

- The response includes `ids`, `records`, and `verified: true` when the SMT proof verifies against the trusted `temporary_root`.
- `limit` is clamped server-side to a safe maximum (currently 100).

#### Server-side filtering + ordering (restricted)

`read-latest` also supports **restricted** server-side queries:

- `where`: equality-only filters (`{ "field": value }`)
- `order_by`: `{ "field": "some_column", "direction": "asc" | "desc" }`

Example:

```bash
curl -sS -X POST "http://localhost:3000/api/models/agents/read-latest" \
  -H "Content-Type: application/json" \
  -d '{
    "limit": 10,
    "where": { "poll_duration": "1440", "enabled": "true" },
    "order_by": { "field": "id", "direction": "desc" }
  }'
```

### Upsert (update semantics)

Apps often need “current value” tables (rate limits, cursors, since_id, etc.). For that, use:

- `POST /api/models/{model}/upsert`

Notes:

- Each record **must include the model’s primary key field**.
- Implemented as `INSERT .. ON CONFLICT(pk) DO UPDATE ..` and still updates/verifies the SMT transition.

Example:

```bash
curl -sS -X POST "http://localhost:3000/api/models/agents/upsert" \
  -H "Content-Type: application/json" \
  -d '{ "records": [ { "id": 1, "poll_duration": "1440" } ] }'
```

### Server-side type coercion + validation errors

The server tries to reduce client/LLM friction by coercing common scalar values based on the model’s column types, e.g.:

- `"1440"` → integer columns
- `"true"` / `"false"` / `"1"` / `"0"` → boolean columns

If coercion/validation fails, the API returns a structured error list (record `index` + `field`):

```json
{
  "success": false,
  "data": {
    "errors": [
      { "index": 0, "field": "poll_duration", "expected": "int", "got": "string", "value": "abc" }
    ]
  },
  "error": "Validation/coercion failed"
}
```

### Root consistency / preventing drift (important)

Earlier versions of this PoC could exhibit an inconsistency where **a Postgres row was written** but the **verifiable state transition failed** (proof verification failed), leading to root drift and “one write succeeds, the next write fails” symptoms.

This repo now applies the following safety fixes:

- **Single-writer root lock**: the service uses a single in-process lock to serialize the entire write critical section so writes cannot interleave with each other or with the background commit task.
- **Atomic verifiable writes (rollback on proof failure)**: `create-batch` and `upsert` keep the SQL transaction open until the SMT proof is verified. If verification fails, the transaction is rolled back so **no DB row persists**.
- **Single API instance enforced by default**: the service takes a Postgres advisory lock on startup; a second instance against the same Postgres will fail fast.
  - Override (not recommended): set `ALLOW_MULTI_INSTANCE=true`.
- **Optional optimistic concurrency**: write requests accept `expected_root` (hex string). If it doesn’t match the current trusted `temporary_root`, the API returns **409** with `code: ROOT_CHANGED` so clients can retry cleanly.
- **Repair path**: if you ever suspect drift, rebuild SMT from DB rows and force-set roots with:
  - `POST /bootstrap/repair-roots` with `{ "confirm": true }`

### Clear data (reset tables + SMT + roots)

To let a client wipe its data (single-tenant/dev flow), call:

- `POST /bootstrap/clear-data`

Request body:

```json
{ "confirm": true }
```

This will:

- Delete rows from all client-managed tables (registered in `verifiable_models`)
- Clear SMT persistence (`merkle_nodes`) and reset the in-memory SMT store
- Reset both `temporary_root` and the on-chain `main_root` to zero

## How to Run

This project runs against the **public Solana devnet** for a realistic, production-like simulation.

### Devnet vs Mainnet (and devnet limitations)

- **Devnet (recommended for this repo right now / PoC)**:
  - Fast iteration and debugging.
  - Free test SOL via `solana airdrop`.
  - Good for integration testing and validating the end-to-end cryptographic flow.
- **Devnet limitations (important)**:
  - **Not a production trust anchor**: devnet is a testing cluster, not intended for long-term guarantees.
  - **State can be reset / pruned**: history and accounts are not guaranteed to be preserved forever.
  - **Reliability is best-effort**: outages, rate limits, and RPC instability can happen.
  - **No economic security**: it does not provide the same “real-value” finality assumptions as mainnet.
  - **Program IDs may change** if you redeploy; your service must be configured with the correct `SOLANA_PROGRAM_ID`.
- **Mainnet (future / optional)**:
  - Use it when the system is production-ready and you need a durable, externally verifiable audit trail.
  - Requires ops/security work: key management, cost model for writes, monitoring/runbooks, and a hardened deployment story.

### 1. Start Dependencies

You only need the PostgreSQL database. The Solana devnet is already running.

*   **PostgreSQL Database**:
    ```bash
    ./scripts/start_db.sh
    ```
    
    Or manually:
    ```bash
    docker run --name pg-verifiable-memory -e POSTGRES_PASSWORD=password -e POSTGRES_DB=verifiable_memory -p 5432:5432 -d postgres
    ```

### 2. Configure Your Environment

Create a `.env` file in the root of the project with the following content:

```env
DATABASE_URL="postgres://postgres:password@localhost:5432/verifiable_memory"
SOLANA_RPC_URL="https://api.devnet.solana.com"
# Program ID from `anchor deploy`
SOLANA_PROGRAM_ID="6fSQZwqdsr8zVSbE8DTo4tsHDW4af3iZyB5KGzEGqyW8"
# Number of temporary_root updates before committing to blockchain (default: 10)
BATCH_COMMIT_SIZE=10
```

You also need to ensure your Solana CLI is configured for devnet and you have some devnet SOL.

*   **Set CLI to Devnet:**
    ```bash
    solana config set --url https://api.devnet.solana.com
    ```
*   **Get Free Devnet SOL (if needed):**
    ```bash
    solana airdrop 2
    ```

### 3. Deploy the Smart Contract

Before running the app, you need to deploy the smart contract to the devnet.

```bash
# Navigate to the program directory
cd solana_program

# This command will build and deploy the contract.
# It will output a new Program ID.
anchor deploy
```

**IMPORTANT**: After deploying, you must copy the new Program ID from the output and update it in these two files:
*   `solana_program/programs/verifiable_db_program/src/lib.rs` (in the `declare_id!` macro)
*   Your `.env` file (set `SOLANA_PROGRAM_ID=<new_program_id>` for the verifiable service)

After updating the IDs, run `anchor deploy` one more time.

### 4. Run the Simulation

Now you can run the simulation against the live devnet.

```bash
# Navigate back to the project root
cd ..

# Run the simulation
./scripts/start_simulation.sh
```

### Running the API Server in Docker (deployment-like)

If you want the API to run **inside Docker** (closer to a deployment model), use the helper scripts in `scripts/`.

#### One-command bring-up (recommended)

```bash
# First time only (make scripts executable)
chmod +x scripts/*.sh

# Start DB + run Solana preflight + start API in Docker
./scripts/docker_up.sh
```

What `docker_up.sh` does (purposefully “fail-fast”):

- **Starts Postgres** using `scripts/start_db.sh` (or reuses the existing container)
- **Validates env vars** from `.env` are present (`DATABASE_URL`, `SOLANA_RPC_URL`, `SOLANA_PROGRAM_ID`, `BATCH_COMMIT_SIZE`)
- **Validates Solana connectivity + program + PDA** using a Rust preflight (`cargo run --bin preflight`)
- **Starts the API service inside Docker** using `scripts/start_api_docker.sh`

#### Network gotcha: Solana RPC from Docker bridge

On some Linux setups, the API container may fail to reach `SOLANA_RPC_URL` when running on the default Docker bridge network (you’ll see startup failures while initializing `RootManager`).

If that happens, run with host networking:

```bash
NETWORK_MODE=host BUILD=false ./scripts/docker_up.sh
```

Notes:

- `NETWORK_MODE=host` is **Linux-only**.
- With host networking, the API uses your host network stack (often fixing DNS/egress issues to Solana RPC).

#### Trusted state persistence (important)

The verifiable service uses `trusted_state.json` as durable “trusted local storage” for crash recovery.

When running via `scripts/start_api_docker.sh`, this file is **bind-mounted from your repo** into the container, so it persists across container restarts:

- Host file: `./trusted_state.json`
- Container path: `/app/trusted_state.json`

#### Build behavior (optional)

`scripts/start_api_docker.sh` supports:

- `BUILD=auto` (default): build only if the image is missing
- `BUILD=true`: always rebuild the API image
- `BUILD=false`: never build (expects the image to already exist)

### Running the API Server Independently

You can also run just the API server with its Swagger UI. It will connect to whichever Solana cluster is defined in your `.env` file.

```bash
cargo run --bin api_server
```
*   **Swagger UI**: [http://localhost:3000/swagger-ui](http://localhost:3000/swagger-ui)

---

## Client-defined schema example (bootstrap/apply-schema)

The service supports **dynamic application tables**. A client can define the database structure by calling:

- `POST /bootstrap/apply-schema`

Example payload (creates an `agents` table):

```json
{
  "tables": [
    {
      "table_name": "agents",
      "primary_key_field": "id",
      "primary_key_kind": "serial",
      "columns": [
        { "name": "name", "col_type": "text", "unique": true, "nullable": false },
        { "name": "description", "col_type": "text", "nullable": true },
        { "name": "metadata", "col_type": "jsonb", "nullable": true },
        { "name": "created_at", "col_type": "timestamptz", "nullable": false }
      ]
    }
  ],
  "force_reset": false
}
```

### Auto-generated `created_at` timestamps (like autogenerated IDs)

If you include a `created_at` column with:

- `col_type: "timestamptz"`
- `nullable: false`
- `name: "created_at"`

…the server-generated DDL will automatically add `DEFAULT now()` for that column:

- `created_at timestamptz NOT NULL DEFAULT now()`

This means clients can omit `created_at` in `create-batch`. The service hashes the **DB-returned row** (from `RETURNING row_to_json(...)`), so the generated timestamp is included in the leaf hash.

Apply it with curl:

```bash
curl -sS -X POST "http://localhost:3000/bootstrap/apply-schema" \
  -H "Content-Type: application/json" \
  -d @schema.json
```

After applying schema, you can:

- Inspect the current DB schema: `GET /bootstrap/schema`
- Write records: `POST /api/models/agents/create-batch`
- Read records: `POST /api/models/agents/read-batch`
- Read latest records: `POST /api/models/agents/read-latest`

Important behavior (single-tenant safety mode): if the `schema_hash` changes, the service will **reset verifiable state** (drop managed tables, clear SMT persistence, and reset roots). Use `force_reset=true` to force a reset even if the schema hash matches.

### Common bootstrap JSON errors (enum values)

The schema types use **snake_case** enums. For example:

- `primary_key_kind: "big_serial"` (NOT `"bigserial"`)

If you send the wrong enum string, you’ll see:

- `Invalid JSON body: Failed to deserialize the JSON body into the target type`


---

## Production Deployment Considerations

While this Proof of Concept demonstrates the core mechanics of a verifiable database, a production-grade deployment would require additional security layers to protect against a malicious or compromised host environment. The following architecture outlines a robust approach for achieving end-to-end security.

### 1. Securing Data-at-Rest: Encrypted Storage with KMS

The database contains the agent's sensitive state. To protect this data from being stolen or inspected by the host operator, it must be encrypted at rest.

*   **Mechanism**: The TEE's virtual disk or file system (where PostgreSQL or another database stores its files) is encrypted using a standard like LUKS.
*   **Key Management**: The decryption key is not stored locally. Instead, it is managed by a **Key Management Service (KMS)**.
*   **Attestation-Gated Access**: The TEE instance is configured to perform **remote attestation** with the KMS upon startup. It generates a cryptographic quote proving it is a genuine TEE running the expected, unmodified code. The KMS only releases the disk decryption key to the TEE after successfully verifying this quote.

This flow ensures that the sensitive database files can only be decrypted and accessed by a legitimate, trusted TEE instance, effectively preventing offline data theft.

### 2. Securing the Audit Trail: TEE-Signed Commits

To guarantee that the Merkle roots committed to the blockchain are authentic and originate from a trusted TEE, all on-chain transactions must be signed by a unique, TEE-bound key.

*   **TEE Key (`PK_TDX`)**: Upon its first launch, the TEE generates a unique, persistent key pair. The private key is sealed and never leaves the enclave. The public key (`PK_TDX`) serves as its cryptographic identity.
*   **On-Chain Key Registration**: A one-time setup process occurs where a trusted **attestation service** verifies the TEE's quote and registers its public key (`PK_TDX`) in the Solana smart contract. This links the TEE's identity to the on-chain program.
*   **Signed State Updates**: From then on, every transaction that commits a new `main_root` to the blockchain must be signed with the TEE's private key. The smart contract verifies this signature against the registered `PK_TDX` before accepting the update.

This architecture ensures that only the authenticated TEE can update the on-chain state, preventing a malicious host from injecting fraudulent Merkle roots and creating a non-repudiable, fully verifiable audit trail.

### Summary of Guarantees

By combining these two mechanisms, the system achieves:

*   **Confidentiality and Integrity**: Data is protected from host access by TEE isolation and disk encryption.
*   **Durability**: The database's own crash-recovery mechanisms (like WAL) ensure state is preserved across restarts.
*   **Verifiable History & Tamper-Detection**: The on-chain, TEE-signed Merkle roots provide a secure, auditable history of the database state, making host-level attacks detectable.

