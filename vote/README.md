# vote

vote has three steps:
1. proposer create vote
2. user vote
3. platform get vote result

## create vote

proposer edit vote info, include:
1. proposal
2. candidates

platform supply:
1. white list of users who can vote
2. start and end time (ckb epoch) of vote
3. build merkle tree with white list, and calc merkel root hash.

finally, fill vote meta:
```
table VoteMeta {
    smt_root_hash: BytesOpt,
    candidates: StringVec,
    start_time: Uint64,
    end_time: Uint64,
    extra: BytesOpt,
}
```
proposal hash as extra.

put VoteMeta into a cell, and send tx, get outpoint of this cell.


## user vote

1. user connect wallet.
2. if user in white list, get a merkel tree proof.
3. build a tx, include:
    1. vote meta cell as celldep.
    2. type script: codehash is vote type script. args is blake160 hash of vote meta cell out point.
    2. data of vote cell: which candidate user vote for.
    3. witness: merkel tree proof and user lockhash.
    ```
    table VoteProof {
        lock_script_hash: Bytes,
        smt_proof: Bytes,
    }
    ```
4. build another tx to destory vote cell.
5. send tx.

## get vote result

1. indexer server scan tx, and check it.
2. if tx is vote tx, update vote result.

