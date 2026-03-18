# create vote cell

example frontend code for create vote cell.


```bash
npm install

npm start
```

### example

```
$ npm start

vote data:  0x04000000
vote proof:  0x540000000c0000002c000000abbfbf9155679b5d1399b4aa04dac6f3f71e63fd3ae4759110a415169eefeeed240000004c4fff50378f5d8873eed84dbfd327bd9b258f2e0a262364b6fd5cb0550e799dba067da4
vote meta:  VoteMeta {
  smt_root_hash: '0x7e602b84ea55d05337c674f99c279b674a454c7186f0b8fc308291783dd59245',
  candidates: [ 'no', 'not bad', 'good', 'awesome' ],
  start_time: 2305843009213694052n,
  end_time: 2305843009213694094n,
  extra: '0x7ecd3c88a1ee095f66ac5690edc9b67feec822bc118c22c7ff67c99513ac107e'
}
vote type args:  0x456c88d8a9569d4881df935e4674a2fc96148917
signed tx:  {"version":"0x0","cellDeps":[{"outPoint":{"txHash":"0x024ec56c1d2ad4940a96edfd5cfd736bdb0c7d7342da9e74d3033872bdb9cbc1","index":"0x0"},"depType":"code"},{"outPoint":{"txHash":"0xe3d16e23410919a671de30ca56902cd44a13c6e0ba56538e09df436e32b9fe00","index":"0x0"},"depType":"code"},{"outPoint":{"txHash":"0xf8de3bb47d055cdf460d93a2a6e1b05f7432f9777c8c474abf4eec1d4aee5d37","index":"0x0"},"depType":"depGroup"}],"headerDeps":[],"inputs":[{"previousOutput":{"txHash":"0x48ed5f4ac5ae40802839ed1ea8807c11ae4561fdf12f08418973716ab20b284a","index":"0x0"},"since":"0x0","cellOutput":{"capacity":"0x16b969d00","lock":{"codeHash":"0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8","hashType":"type","args":"0xdc3ff72c77f90a034b69b593f6b339ced1d85de8"}},"outputData":"0x"},{"previousOutput":{"txHash":"0x48ed5f4ac5ae40802839ed1ea8807c11ae4561fdf12f08418973716ab20b284a","index":"0x1"},"since":"0x0","cellOutput":{"capacity":"0x3e95ba134","lock":{"codeHash":"0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8","hashType":"type","args":"0xdc3ff72c77f90a034b69b593f6b339ced1d85de8"}},"outputData":"0x"}],"outputs":[{"capacity":"0x2bf55b600","lock":{"codeHash":"0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8","hashType":"type","args":"0xdc3ff72c77f90a034b69b593f6b339ced1d85de8"},"type":{"codeHash":"0xb140de2d7d1536cfdcb82da7520475edce5785dff90edae9073c1143d88f50c5","hashType":"type","args":"0x456c88d8a9569d4881df935e4674a2fc96148917"}},{"capacity":"0x2959c7e93","lock":{"codeHash":"0x9bd7e06f3ecf4be0f2fcd2188b23f1b9fcc88e5d4b65a8637b17723bbda3cce8","hashType":"type","args":"0xdc3ff72c77f90a034b69b593f6b339ced1d85de8"}}],"outputsData":["0x04000000","0x"],"witnesses":["0xad000000100000005500000055000000410000005df6882b487fa63d09323d40403542fc66645efc6f97fad6b9fc680f1852a7ec633c37fbb2098ff2a6674a4084e2bc498fcb92632a28d8520441fec97098751c0054000000540000000c0000002c000000abbfbf9155679b5d1399b4aa04dac6f3f71e63fd3ae4759110a415169eefeeed240000004c4fff50378f5d8873eed84dbfd327bd9b258f2e0a262364b6fd5cb0550e799dba067da4"]}
vote transaction hash is 0x73e4dfc48b629b5876e0bcf84143bc6bc15310fe215b37fc5f45bbcea89550ea
```