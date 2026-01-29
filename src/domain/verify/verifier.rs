// This file is used to verify the proof of the SMT.

use crate::storage::smt::{h256_to_smt, SmtBlake2bHasher};
use primitive_types::H256;
use sparse_merkle_tree::MerkleProof;

/// Verifies a Merkle proof for a set of key-value pairs against a trusted root.
/// This function would run inside the TEE.
pub fn verify_smt_proof(root: H256, leaves: Vec<(H256, H256)>, proof: MerkleProof) -> bool {
    let root_smt = h256_to_smt(root);
    let leaves_smt = leaves
        .into_iter()
        .map(|(k, v)| (h256_to_smt(k), h256_to_smt(v)))
        .collect();

    proof
        .verify::<SmtBlake2bHasher>(&root_smt, leaves_smt)
        .is_ok()
}

/// Verifies a Merkle proof for a state transition (an update to a key-value pair).
#[allow(dead_code)] // Reserved for future use
pub fn verify_smt_proof_of_update(
    trusted_root: H256,
    proposed_root: H256,
    key: H256,
    new_value: H256,
    proof: MerkleProof,
) -> bool {
    // For a new key, the "before" value is the zero hash.
    println!("trusted_root: {}", hex::encode(trusted_root.as_bytes()));
    println!("proposed_root: {}", hex::encode(proposed_root.as_bytes()));
    println!("key: {}", hex::encode(key.as_bytes()));
    println!("new_value: {}", hex::encode(new_value.as_bytes()));
    println!("proof: {:?}", proof);
    let old_value = H256::zero();

    let trusted_root_smt = h256_to_smt(trusted_root);
    let key_smt = h256_to_smt(key);
    let old_value_smt = h256_to_smt(old_value);
    let new_value_smt = h256_to_smt(new_value);

    // 1. Verify the proof of the *previous* state against the trusted root.
    let calculated_old_root = proof
        .clone()
        .compute_root::<SmtBlake2bHasher>(vec![(key_smt, old_value_smt)])
        .unwrap_or_default();
    println!(
        "TEE calculated old root: {}",
        hex::encode(calculated_old_root.as_slice())
    );
    if calculated_old_root != trusted_root_smt {
        return false;
    }

    // 2. Compute the new root with the updated value and verify it matches the proposed root.
    let calculated_new_root = proof
        .compute_root::<SmtBlake2bHasher>(vec![(key_smt, new_value_smt)])
        .unwrap_or_default();
    println!(
        "TEE calculated new root: {}",
        hex::encode(calculated_new_root.as_slice())
    );

    let proposed_root_smt = h256_to_smt(proposed_root);

    calculated_new_root == proposed_root_smt
}

/// Verifies a Merkle proof for a batch state transition.
pub fn verify_smt_multi_update_proof(
    trusted_root: H256,
    proposed_root: H256,
    keys: Vec<H256>,
    new_values: Vec<H256>,
    proof: MerkleProof,
) -> bool {
    let old_value = H256::zero();
    let trusted_root_smt = h256_to_smt(trusted_root);

    let old_leaves_smt: Vec<_> = keys
        .iter()
        .map(|k| (h256_to_smt(*k), h256_to_smt(old_value)))
        .collect();

    let new_leaves_smt: Vec<_> = keys
        .into_iter()
        .zip(new_values.into_iter())
        .map(|(k, v)| (h256_to_smt(k), h256_to_smt(v)))
        .collect();

    // 1. Verify the proof of the *previous* state against the trusted root.
    let calculated_old_root = proof
        .clone()
        .compute_root::<SmtBlake2bHasher>(old_leaves_smt)
        .unwrap_or_default();
    if calculated_old_root != trusted_root_smt {
        return false;
    }

    // 2. Compute the new root with the updated values and verify it matches the proposed root.
    let calculated_new_root = proof
        .compute_root::<SmtBlake2bHasher>(new_leaves_smt)
        .unwrap_or_default();

    let proposed_root_smt = h256_to_smt(proposed_root);

    calculated_new_root == proposed_root_smt
}

/// Verifies a Merkle proof for a batch state transition using explicit old leaf values.
///
/// This is required for updates/upserts where the prior leaf value may NOT be zero.
pub fn verify_smt_multi_update_proof_with_old_values(
    trusted_root: H256,
    proposed_root: H256,
    keys: Vec<H256>,
    old_values: Vec<H256>,
    new_values: Vec<H256>,
    proof: MerkleProof,
) -> bool {
    if keys.len() != old_values.len() || keys.len() != new_values.len() {
        return false;
    }

    let trusted_root_smt = h256_to_smt(trusted_root);

    let old_leaves_smt: Vec<_> = keys
        .iter()
        .copied()
        .zip(old_values.into_iter())
        .map(|(k, v)| (h256_to_smt(k), h256_to_smt(v)))
        .collect();

    let new_leaves_smt: Vec<_> = keys
        .into_iter()
        .zip(new_values.into_iter())
        .map(|(k, v)| (h256_to_smt(k), h256_to_smt(v)))
        .collect();

    let calculated_old_root = proof
        .clone()
        .compute_root::<SmtBlake2bHasher>(old_leaves_smt)
        .unwrap_or_default();
    if calculated_old_root != trusted_root_smt {
        return false;
    }

    let calculated_new_root = proof
        .compute_root::<SmtBlake2bHasher>(new_leaves_smt)
        .unwrap_or_default();

    let proposed_root_smt = h256_to_smt(proposed_root);
    calculated_new_root == proposed_root_smt
}
