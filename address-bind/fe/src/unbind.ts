import { exit } from "process";
import { BindInfo, BindInfoWithSig } from "./molecules";
import { ccc, WitnessArgs } from "@ckb-ccc/core";

async function main() {
  // generate unbind info
  // reuse BindInfo
  // to script of the address to unbind, sig is empty.
  const cccClient = new ccc.ClientPublicTestnet();
  const signer = new ccc.SignerCkbPrivateKey(cccClient, '0x88179b7e387921a193f459859d8ff461e973a93a449b4930179928dbc53a04ba');

  const toAddress =
    "ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah";

  const fromAddress =
    "ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah";

  const fromAddr = await ccc.Address.fromString(fromAddress, cccClient);

  const toAddr = await ccc.Address.fromString(toAddress, cccClient);

  const timeNow = Date.now();

  console.log("time now: ", timeNow);

  const unbindInfoLike = {
    to: fromAddr.script,
    timestamp: BigInt(timeNow)
  }

  const unbindInfo = BindInfo.from(unbindInfoLike);

  const unbindInfoBytes = unbindInfo.toBytes();

  const unbindInfoHex = ccc.hexFrom(unbindInfoBytes);

  console.log("unbind info: ", unbindInfoHex);


  const unbindInfoWithSig = BindInfoWithSig.from({
    bind_info: unbindInfoLike,
    sig: "0x"
  });

  const unbindInfoWithSigBytes = unbindInfoWithSig.toBytes();

  const unbindInfoWithSigHex = ccc.hexFrom(unbindInfoWithSigBytes);

  console.log("unbind info with sig: ", unbindInfoWithSigHex);

  // toAddr transfer some ckb to itself
  const tx = ccc.Transaction.default();
  await tx.completeInputsAtLeastOne(signer);
  await tx.completeFeeBy(signer);

  // set unbind info with sig into witness
  let witnessArgs = WitnessArgs.from({
    inputType: unbindInfoWithSigBytes,  
  });
  tx.setWitnessArgsAt(0, witnessArgs);

  await signer.signTransaction(tx);

  console.log("tx: ", ccc.stringify(tx));

  // just for test no need send tx
  const txHash = await signer.sendTransaction(tx);
  console.log("The transaction hash is", txHash);
}

main().then(() => process.exit());
