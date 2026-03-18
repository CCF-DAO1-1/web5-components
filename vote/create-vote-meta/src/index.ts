import { exit } from "process";
import { VoteMeta, VoteMetaLike } from "./molecules";
import { ccc, hexFrom, Transaction, WitnessArgs, hashCkb, SinceLike, Since} from "@ckb-ccc/core";

async function main() {
  const cccClient = new ccc.ClientPublicTestnet();
  const signer = new ccc.SignerCkbPrivateKey(cccClient, '0x88179b7e387921a193f459859d8ff461e973a93a449b4930179928dbc53a04ba');

  const proposerAddress =
    "ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah";

  const proposerAddr = await ccc.Address.fromString(proposerAddress, cccClient);

  // proposer edit
  const proposal = "is this a good idea?"
  const encoder = new TextEncoder();
  const proposalHash = hashCkb(encoder.encode(proposal));
  const candidates = ["no", "not bad", "good", "awesome"]

  // platform supply
  const whiteList = [
        "0x1380b63765aacc617854fd83434a90caa535ba7117fbf3d9d01ba7f1d472b561",
        "0xe20182d70b2c498483d626f9f558050a8e0df731bbafdfa277e34e3a0a745036",
        "0x7c9b0e612253e09f74501e84982cedbf099ee836f158de22d4a294c9e43a294b",
        "0xeb5b414a4db5f571990ad0912d14b0711f88720ea9b1ca67d59f09a44b879071",
        "0xabbfbf9155679b5d1399b4aa04dac6f3f71e63fd3ae4759110a415169eefeeed"
  ]

  // get from smt component
  const smtRootHash = "0x7e602b84ea55d05337c674f99c279b674a454c7186f0b8fc308291783dd59245"

  // since format
  // from epoch 100 to 142
  const startTime: SinceLike = {
    relative: "absolute",
    metric: "epoch",
    value: 100,
  }
  const startSince = Since.from(startTime);
  const startTimeNum = startSince.toNum();
  const endTime: SinceLike = {
    relative: "absolute",
    metric: "epoch",
    value: 142,
  }
  const endSince = Since.from(endTime);
  const endTimeNum = endSince.toNum();

  const voteMetaLike = {
    smt_root_hash: smtRootHash,
    candidates,
    start_time: startTimeNum,
    end_time: endTimeNum,
    extra: proposalHash,
  }

  const voteMeta = VoteMeta.from(voteMetaLike);

  const voteMetaBytes = voteMeta.toBytes();

  const voteMetaHex = ccc.hexFrom(voteMetaBytes);

  console.log("vote meta: ", voteMetaHex);

  // create a cell which data is vote meta
  const tx = Transaction.from({
    outputs: [
      {
        lock: proposerAddr.script,
        type: null,
      }
    ],
    outputsData: [voteMetaHex],
  })
  await tx.completeInputsByCapacity(signer);
  await tx.completeFeeBy(signer);

  // await signer.signTransaction(tx);

  console.log("tx: ", ccc.stringify(tx));
  // console.log("tx hash: ", tx.hash());

  // just for test no need send tx
  const txHash = await signer.sendTransaction(tx);
  console.log("The transaction hash is", txHash);
}

main().then(() => process.exit());
