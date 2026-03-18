import {
  mol,
  HexLike,
  hexFrom,
  Hex,
  NumLike,
  numFrom,
  Num,
  Script,
  ScriptLike,
} from "@ckb-ccc/core";


// table VoteProof {
//     lock_script_hash: Byte32,
//     smt_proof: Bytes,
// }

export type VoteProofLike = {
  lock_script_hash: HexLike;
  smt_proof: HexLike;
};

@mol.codec(
  mol.table({
    lock_script_hash: mol.Byte32,
    smt_proof: mol.Bytes,
  }),
)
export class VoteProof extends mol.Entity.Base<VoteProofLike, VoteProof>() {
  constructor(
    public lock_script_hash: HexLike,
    public smt_proof: HexLike,
  ) {
    super();
  }

  static from(data: VoteProofLike): VoteProof {
    if (data instanceof VoteProof) {
      return data;
    }
    return new VoteProof(
      hexFrom(data.lock_script_hash),
      hexFrom(data.smt_proof),
    );
  }
}


// table VoteMeta {
//     smt_root_hash: Byte32Opt,
//     candidates: StringVec,
//     start_time: Uint64,
//     end_time: Uint64,
//     extra: BytesOpt,
// }

export type VoteMetaLike = {
  smt_root_hash: HexLike | null;
  candidates: string[];
  start_time: NumLike;
  end_time: NumLike;
  extra: HexLike | null;
};

@mol.codec(
  mol.table({
    smt_root_hash: mol.Byte32Opt,
    candidates: mol.StringVec,
    start_time: mol.Uint64,
    end_time: mol.Uint64,
    extra: mol.BytesOpt,
  }),
)
export class VoteMeta extends mol.Entity.Base<VoteMetaLike, VoteMeta>() {
  constructor(
    public smt_root_hash: HexLike | null,
    public candidates: string[],
    public start_time: NumLike,
    public end_time: NumLike,
    public extra: HexLike | null,
  ) {
    super();
  }

  static from(data: VoteMetaLike): VoteMeta {
    if (data instanceof VoteMeta) {
      return data;
    }
    return new VoteMeta(
      data.smt_root_hash ? hexFrom(data.smt_root_hash) : null,
      data.candidates,
      numFrom(data.start_time),
      numFrom(data.end_time),
      data.extra ? hexFrom(data.extra) : null,
    );
  }
}