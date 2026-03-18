import { ccc, Transaction } from "@ckb-ccc/core";

async function main() {
  const cccClient = new ccc.ClientPublicTestnet();
  const signer = new ccc.SignerCkbPrivateKey(cccClient, '0x88179b7e387921a193f459859d8ff461e973a93a449b4930179928dbc53a04ba');

    const addrStr =
    "ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah";

  const addr = await ccc.Address.fromString(addrStr, cccClient);
  
  // destroy vote cell to get back ckb
  const destroyTx = Transaction.from({
    inputs: [
      {
        previousOutput: {
          txHash: "0x109b4b301fe2c21942422780c19f40a53881d74f0312516fc225f8d6f12d9933",
          index: 0,
        },
        since: 0,
      }
    ],
    outputs: [
      {
        lock: addr.script,
        type: null,
      }
    ],
    outputsData: ["0x"],
  })
  await destroyTx.completeInputsByCapacity(signer);
  await destroyTx.completeFeeBy(signer);
  const destroyTxHash = await signer.sendTransaction(destroyTx);
  console.log("destroy tx hash: ", destroyTxHash);
}

main().then(() => process.exit());