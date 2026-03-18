import { exit } from "process";
import { VoteProof, VoteMeta } from "./molecules";
import { ccc, Transaction, WitnessArgs, OutPoint} from "@ckb-ccc/core";

async function main() {
  const cccClient = new ccc.ClientPublicTestnet();
  const signer = new ccc.SignerCkbPrivateKey(cccClient, '0x88179b7e387921a193f459859d8ff461e973a93a449b4930179928dbc53a04ba');

  const voteAddress =
    "ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah";

  const voteAddr = await ccc.Address.fromString(voteAddress, cccClient);

  // vote "good"
  const candidates = ["no", "not bad", "good", "awesome"]
  const voteIndex = candidates.indexOf("good");
  const voteNum = 1 << voteIndex; 
  // voteData is 32 bit little endian
  const voteData = voteNum.toString(16).padStart(8, "0")
    .match(/.{2}/g)!
    .reverse()
    .join("");
  console.log("vote data: ", ccc.hexFrom(voteData));

  // user smt proof from smt
  const userSmtProof = "0x4c4fff50378f5d8873eed84dbfd327bd9b258f2e0a262364b6fd5cb0550e799dba067da4";
  // build vote proof
  const voteProof = new VoteProof(
    voteAddr.script.hash(),
    userSmtProof,
  )
  const voteProofBytes = voteProof.toBytes();
  const voteProofHex = ccc.hexFrom(voteProofBytes);
  console.log("vote proof: ", voteProofHex);

  // cell deps of vote meta
  const voteMetaOutpoint = {
    txHash: "0xe3d16e23410919a671de30ca56902cd44a13c6e0ba56538e09df436e32b9fe00",
    index: 0,
  }
  const voteMetaCellDep = {
    outPoint: voteMetaOutpoint,
    depType: "code",
  }


  // get cell data to show vote meta
  const voteMetaCell = await cccClient.getCell(voteMetaOutpoint);
  if (!voteMetaCell) {
    console.log("vote meta cell not found");
    exit(1);
  }
  const voteMeta = VoteMeta.fromBytes(voteMetaCell.outputData)
  console.log("vote meta: ", voteMeta);


  // typescript of vote contract
  // from https://github.com/XuJiandong/ckb-dao-vote/blob/main/docs/ckb-dao-vote.md
  // args is blake160 hash of vote meta cell out point
  // cell deps of vote meta
  const voteContractOutpoint = {
    txHash: "0x024ec56c1d2ad4940a96edfd5cfd736bdb0c7d7342da9e74d3033872bdb9cbc1",
    index: 0,
  }
  const voteContractCellDep = {
    outPoint: voteContractOutpoint,
    depType: "code",
  }

  const voteTypeArgs = OutPoint.from(voteMetaOutpoint).hash().slice(0, 42);
  console.log("vote type args: ", voteTypeArgs);
  const voteTypeScript = {
    codeHash: "0xb140de2d7d1536cfdcb82da7520475edce5785dff90edae9073c1143d88f50c5",
    args: voteTypeArgs,
    hashType: "type",
  }

  // create a vote cell
  const tx = Transaction.from({
    cellDeps: [
      voteContractCellDep,
      voteMetaCellDep,
    ],
    outputs: [
      {
        lock: voteAddr.script,
        type: voteTypeScript,
      }
    ],
    outputsData: [voteData],
  })

    // set vote proof into witness
  let witnessArgs = WitnessArgs.from({
    outputType: voteProofBytes,
  });

  await tx.completeInputsByCapacity(signer);
  await tx.completeFeeBy(signer);

  const witness = WitnessArgs.fromBytes(tx.witnesses[0]);
  
  witness.outputType = ccc.hexFrom(voteProofBytes);
  tx.witnesses[0] = ccc.hexFrom(witness.toBytes());

  const signedTx = await signer.signTransaction(tx);

  console.log("signed tx: ", ccc.stringify(signedTx));

  const txHash = await cccClient.sendTransaction(signedTx);
  console.log("vote transaction hash is", txHash);
}

main().then(() => process.exit());
