import * as anchor from "@coral-xyz/anchor";
import { Program } from "@coral-xyz/anchor";
import { VerifiableDbProgram } from "../target/types/verifiable_db_program";

describe("verifiable_db_program", () => {
  // Configure the client to use the local cluster.
  anchor.setProvider(anchor.AnchorProvider.env());

  const program = anchor.workspace.verifiableDbProgram as Program<VerifiableDbProgram>;

  it("Is initialized!", async () => {
    const initialRoot = new Array(32).fill(0); // [u8;32]
    const tx = await program.methods.initialize(initialRoot).rpc();
    console.log("Your transaction signature", tx);
  });
});
