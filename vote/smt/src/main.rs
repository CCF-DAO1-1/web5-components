mod smt_hasher;

use clap::{Parser, Subcommand};
use smt_hasher::Blake2bHasher;
use sparse_merkle_tree::{default_store::DefaultStore, SparseMerkleTree, H256, CompiledMerkleProof};

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long, default_value = "example/white_list.json")]
    white_list: String,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Proof {
        #[arg(short, long)]
        lock_hash: String,
    },
}

const SMT_VALUE: [u8; 32] = [
    1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
];

pub type CkbSMT = SparseMerkleTree<Blake2bHasher, H256, DefaultStore<H256>>;

fn main() {
        let cli = Cli::parse();

    match cli.command {
        Commands::Proof { lock_hash } => {
            let mut lock_hash = lock_hash;
            if lock_hash.starts_with("0x") {
                lock_hash = lock_hash[2..].to_string();
            }
            let lock_hash = hex::decode(lock_hash).unwrap();

            let white_list = std::fs::read_to_string(cli.white_list).unwrap();
            let white_list: serde_json::Value = serde_json::from_str(&white_list).unwrap();
            let white_list = white_list["white_list"].as_array().unwrap();
            
            // get all values in white_list and build merkle tree
            let mut smt_tree = CkbSMT::default();
            for lock_hash_item in white_list {
                let mut lock_hash_item = lock_hash_item.as_str().unwrap().to_string();
                if lock_hash_item.starts_with("0x") {
                    lock_hash_item = lock_hash_item[2..].to_string();
                }
                println!("insert lock_hash_item: {}", lock_hash_item);
                let lock_hash_item = hex::decode(lock_hash_item).unwrap();
                let key: [u8; 32] = lock_hash_item.as_slice().try_into().unwrap();
                smt_tree
                    .update(key.into(), SMT_VALUE.clone().into())
                    .unwrap();
            }

            let smt_root_hash: H256 = smt_tree.root().clone();
            println!("smt_root_hash: {}", hex::encode(smt_root_hash.as_slice()));

            println!("give proof of lock_hash: {}", hex::encode(&lock_hash));
            let key: [u8; 32] = lock_hash.as_slice().try_into().unwrap();
            let proof = smt_tree.merkle_proof(vec![key.into()]).unwrap();
            let compiled_proof = proof.clone().compile(vec![key.into()]).unwrap();
            println!("proof: {}", hex::encode(&compiled_proof.0));

            // merkle_proof always give proof, even the key is not in the tree
            // so we must verify the proof then return to user
            let proof: Vec<u8> = compiled_proof.0;
            let compiled_proof = CompiledMerkleProof(proof);
            let ret = compiled_proof
                .verify::<Blake2bHasher>(&smt_root_hash.into(), vec![(key.into(), SMT_VALUE.into())]);
            if ret.unwrap() {
                println!("verify success!");
            } else {
                println!("verify fail!");
            }
        }
    }

}
