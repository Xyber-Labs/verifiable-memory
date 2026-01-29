// Solana smart contract for the verifiable database.
use anchor_lang::prelude::*;

declare_id!("6fSQZwqdsr8zVSbE8DTo4tsHDW4af3iZyB5KGzEGqyW8");

#[program]
pub mod verifiable_db_program {
    use super::*;

    pub fn initialize(ctx: Context<Initialize>, initial_root: [u8; 32]) -> Result<()> {
        let merkle_root_account = &mut ctx.accounts.merkle_root_account;
        merkle_root_account.root = initial_root;
        merkle_root_account.timestamp = Clock::get()?.unix_timestamp;
        Ok(())
    }

    pub fn update_root(ctx: Context<UpdateRoot>, new_root: [u8; 32]) -> Result<()> {
        let merkle_root_account = &mut ctx.accounts.merkle_root_account;
        merkle_root_account.root = new_root;
        merkle_root_account.timestamp = Clock::get()?.unix_timestamp;
        Ok(())
    }
}

#[derive(Accounts)]
pub struct Initialize<'info> {
    #[account(
        init_if_needed,
        payer = user,
        space = 8 + 32 + 8,
        seeds = [b"merkle_root_account"],
        bump
    )]
    pub merkle_root_account: Account<'info, MerkleRootAccount>,
    #[account(mut)]
    pub user: Signer<'info>,
    pub system_program: Program<'info, System>,
}

#[derive(Accounts)]
pub struct UpdateRoot<'info> {
    #[account(mut)]
    pub merkle_root_account: Account<'info, MerkleRootAccount>,
    pub user: Signer<'info>,
}

#[account]
pub struct MerkleRootAccount {
    pub root: [u8; 32],
    pub timestamp: i64,
}
