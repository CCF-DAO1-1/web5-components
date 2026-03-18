import { exit } from "process";
import { BindInfo, BindInfoWithSig } from "./molecules";
import { ccc, hexFrom, Transaction, WitnessArgs } from "@ckb-ccc/core";

async function main() {
  // generate bind info
  const cccClient = new ccc.ClientPublicTestnet();
  const signer = new ccc.SignerCkbPrivateKey(cccClient, '0x88179b7e387921a193f459859d8ff461e973a93a449b4930179928dbc53a04ba');

  const toAddress =
    "ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah";

  const toAddr = await ccc.Address.fromString(toAddress, cccClient);

  const timeNow = Date.now();

  console.log("time now: ", timeNow);

  const bindInfoLike = {
    to: toAddr.script,
    timestamp: BigInt(timeNow)
  }

  const bindInfo = BindInfo.from(bindInfoLike);

  const bindInfoBytes = bindInfo.toBytes();

  const bindInfoHex = ccc.hexFrom(bindInfoBytes);

  console.log("bind info: ", bindInfoHex);

  // sign bindInfoHex with Neuron
  let sig = await signer.signMessage(bindInfoHex);
  const sigHex = sig.signature;

  console.log("sig: ", sigHex);

  const bindInfoWithSig = BindInfoWithSig.from({
    bind_info: bindInfoLike,
    sig: hexFrom(sigHex)
  })

  const bindInfoWithSigBytes = bindInfoWithSig.toBytes();

  const bindInfoWithSigHex = ccc.hexFrom(bindInfoWithSigBytes);

  console.log("bind info with sig: ", bindInfoWithSigHex);

  // toAddr transfer some ckb to itself
  const tx = ccc.Transaction.default();
  await tx.completeInputsAtLeastOne(signer);
  await tx.completeFeeBy(signer);

  // set bind info with sig into witness
  let witnessArgs = WitnessArgs.from({
    inputType: bindInfoWithSigBytes,
  });
  tx.setWitnessArgsAt(0, witnessArgs);

  await signer.signTransaction(tx);

  console.log("tx: ", ccc.stringify(tx));

  // just for test no need send tx
  //const txHash = await signer.sendTransaction(tx);
  //console.log("The transaction hash is", txHash);
}

main().then(() => process.exit());
