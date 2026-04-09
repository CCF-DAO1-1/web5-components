use crate::bind::BindInfoWithSig;
use ckb_jsonrpc_types::Either;
use ckb_jsonrpc_types::Transaction;
use ckb_sdk::rpc::CkbRpcClient;
use ckb_sdk::{Address, AddressPayload, NetworkType};
use ckb_types::bytes::Bytes;
use ckb_types::core::ScriptHashType;
use ckb_types::prelude::Entity;
use ckb_types::prelude::Pack;
use ckb_types::{H160, H256, packed};
use molecule::prelude::Builder;
use secp256k1::{Error, Message, PublicKey, ecdsa};
use std::str::FromStr;

pub async fn get_tx(ckb_client: &CkbRpcClient, tx_hash: H256) -> Result<Transaction, String> {
    let tx_either = ckb_client
        .get_transaction(tx_hash)
        .map_err(|e| format!("Failed to get transaction: {e}"))?
        .ok_or("tx not found".to_string())?
        .transaction
        .ok_or("tx not found".to_string())?
        .inner;
    match tx_either {
        Either::Left(tx_view) => {
            let tx = tx_view.inner;
            Ok(tx)
        }
        Either::Right(_) => Err("tx not found".to_string()),
    }
}

fn recover(msg_digest: [u8; 32], sig: [u8; 64], recovery_id: u8) -> Result<PublicKey, Error> {
    let secp = secp256k1::Secp256k1::new();
    let id = ecdsa::RecoveryId::try_from(i32::from(recovery_id))?;
    let sig = ecdsa::RecoverableSignature::from_compact(&sig, id)?;
    let msg = Message::from_digest(msg_digest);

    secp.recover_ecdsa(&msg, &sig)
}

// from address must be secp256/blake160
pub fn calculate_from_address(from_args: &[u8], network: NetworkType) -> Result<Address, String> {
    if from_args.len() != 20 {
        return Err(format!(
            "from_args length must be 20, got {}",
            from_args.len()
        ));
    }
    let code_hash =
        H256::from_str("9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8").unwrap();
    let args = H160::from_slice(from_args).unwrap().as_bytes().to_owned();
    let hash_type = ScriptHashType::Type;
    let lock_script = packed::Script::new_builder()
        .code_hash(code_hash.pack())
        .args(Bytes::from(args).pack())
        .hash_type(hash_type.into())
        .build();
    let payload = AddressPayload::from(lock_script);
    Ok(Address::new(network, payload, true))
}

// to address
pub fn calculate_to_address(
    to_script: &crate::bind::Script,
    network: NetworkType,
) -> Result<Address, String> {
    let code_hash_raw = to_script.code_hash().raw_data();
    if code_hash_raw.len() != 32 {
        return Err(format!(
            "code_hash length must be 32, got {}",
            code_hash_raw.len()
        ));
    }
    let code_hash = H256::from_slice(&code_hash_raw).unwrap();
    let args = to_script.args().raw_data().to_vec();
    let hash_type = to_script.hash_type();
    let lock_script = packed::Script::new_builder()
        .code_hash(code_hash.pack())
        .args(Bytes::from(args).pack())
        .hash_type(hash_type)
        .build();
    let payload = AddressPayload::from(lock_script);
    Ok(Address::new(network, payload, true))
}

pub fn calculate_address(lock_script: &packed::Script, network: NetworkType) -> Address {
    let payload = AddressPayload::from(lock_script.clone());
    Address::new(network, payload, true)
}

pub fn black_hole_address(network: NetworkType) -> Address {
    let code_hash =
        H256::from_str("0000000000000000000000000000000000000000000000000000000000000000").unwrap();
    let args = Bytes::from(vec![]).pack();
    let hash_type = ScriptHashType::Type;
    let lock_script = packed::Script::new_builder()
        .code_hash(code_hash.pack())
        .args(args)
        .hash_type(hash_type.into())
        .build();
    calculate_address(&lock_script, network)
}

// return (type: 0 bind, 1 unbind, from_address, to_address, timestamp)
pub async fn verify_tx(
    ckb_client: &CkbRpcClient,
    network: NetworkType,
    tx: &Transaction,
) -> Result<(u64, String, String, u64), String> {
    let inputs_count = tx.inputs.len();
    let outputs_count = tx.outputs.len();

    // one input, one output
    if inputs_count != 1 || outputs_count != 1 {
        return Err("inputs_count or outputs_count not equal 1".to_string());
    }

    if tx.witnesses.is_empty() {
        return Err("witnesses is empty".to_string());
    }

    // extract bind info with sig from witness
    let witness = tx.witnesses[0].clone();
    let witness_bytes = witness.into_bytes();
    let witness_args = packed::WitnessArgs::from_compatible_slice(&witness_bytes)
        .map_err(|e| format!("parse witness args failed: {e}"))?;

    let input_type = if let Some(input_type) = witness_args.input_type().to_opt() {
        input_type
    } else {
        return Err("input_type is None".to_string());
    };
    let bind_info_with_sig_bytes = input_type.raw_data().to_vec();
    let bind_info_with_sig = BindInfoWithSig::from_compatible_slice(&bind_info_with_sig_bytes)
        .map_err(|e| format!("parse bind info with sig failed: {e}"))?;

    // input lock script must be equal to output lock script
    let pre_tx_hash = tx.inputs[0].previous_output.tx_hash.clone();
    let pre_index: u32 = tx.inputs[0].previous_output.index.into();
    let pre_tx = get_tx(ckb_client, pre_tx_hash)
        .await
        .map_err(|e| format!("get_tx failed: {e}"))?;
    let pre_output = pre_tx
        .outputs
        .get(pre_index as usize)
        .ok_or_else(|| {
            format!(
                "pre_tx output index {} out of bounds, outputs count: {}",
                pre_index,
                pre_tx.outputs.len()
            )
        })?
        .clone();
    let pre_output_lock_script = pre_output.lock.clone();
    let output_lock_script = tx.outputs[0].lock.clone();

    // transfer to itself
    if pre_output_lock_script != output_lock_script {
        return Err("pre_output_lock_script not equal output_lock_script".to_string());
    }

    let bind_info = bind_info_with_sig.bind_info();
    let sig = bind_info_with_sig.sig();
    let sig_bytes = sig.raw_data().to_vec();
    let bind_info_bytes = bind_info.as_slice();
    let timestamp = u64::from_le_bytes(
        bind_info
            .timestamp()
            .as_slice()
            .try_into()
            .map_err(|e| format!("parse timestamp failed: {e}"))?,
    );

    // if sig is empty, it is unbind
    // from addr is bindinfo.to
    // to addr is lock script of tx
    if sig_bytes.is_empty() {
        let to_addr = calculate_address(&output_lock_script.into(), network);
        let from_addr = calculate_to_address(&bind_info.to(), network)?;
        return Ok((1, from_addr.to_string(), to_addr.to_string(), timestamp));
    }

    // transfer is to of bind info
    if bind_info.to().code_hash().raw_data() != output_lock_script.code_hash.as_bytes()
        || u8::from(bind_info.to().hash_type()) != output_lock_script.hash_type.clone() as u8
        || bind_info.to().args().raw_data() != output_lock_script.args.as_bytes()
    {
        return Err("bind_info_to not equal output_lock_script".to_string());
    }

    if sig_bytes.len() != 65 {
        return Err("sig_bytes len not equal 65".to_string());
    }

    // recover from address by sig
    // message is hex string with 0x
    let message = format!("Nervos Message:0x{}", hex::encode(bind_info_bytes));
    let message_hash = ckb_hash::blake2b_256(message.as_bytes());
    let mut sig: [u8; 64] = [0; 64];
    sig.copy_from_slice(&sig_bytes[0..64]);
    let recovery_id: u8 = sig_bytes[64];
    let ret = recover(message_hash, sig, recovery_id);
    let pubkey = match ret {
        Ok(pk) => pk,
        Err(_e) => return Err("recover error".to_string()),
    };
    let pubkey_hash = ckb_hash::blake2b_256(pubkey.serialize());
    let from_args = pubkey_hash[0..20].to_vec();

    let timestamp = u64::from_le_bytes(
        bind_info
            .timestamp()
            .as_slice()
            .try_into()
            .map_err(|e| format!("parse timestamp failed: {e}"))?,
    );
    let from_addr = calculate_from_address(&from_args, network)?;
    let to_addr = calculate_address(&output_lock_script.into(), network);

    Ok((0, from_addr.to_string(), to_addr.to_string(), timestamp))
}
