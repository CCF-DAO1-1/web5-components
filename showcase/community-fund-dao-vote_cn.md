# Community fund DAO投票方案

## 场景

Community fund DAO针对社区发起的提案进行投票。

投票参与人员有限制：需要在Nervos DAO中质押过ckb，投票权重为质押的ckb数量。

要求：有去中心化的需求，要使用链上合约实现投票功能，链上只实现基础投票功能，权重在链外计算。

参见 [Community Fund DAO v1.1 Web5 优化提案](https://www.notion.so/23924205dae080ed9290e95519c57ab1?pvs=21)

## 投票合约spec

[投票合约spec](https://github.com/XuJiandong/ckb-dao-vote/blob/main/docs/ckb-dao-vote.md)

这是一个比较通用的链上投票合约，支持：

- 有白名单/无白名单
- 单选/多选

大致内容如下：

### 发起投票

需要先创建一个vote meta cell，data为如下结构：

```
table VoteMeta {
    smt_root_hash: Byte32Opt,
    candidates: StringVec,
    start_time: Uint64,
    end_time: Uint64,
    extra: BytesOpt,
}
```

smt_root_hash：
* 为空表示本次投票没有白名单限制
* 如果有值，则表示本次投票有白名单限制。其值为所有白名单用户（lock script hash）组成的SMT的root hash。

candidates： 为本次投票的所有可选项的描述，不能超过32个。

start_time： 本次投票的开始时间。建议用since格式，但是合约并不检查，而是留给链下计票服务检查。

end_time： 本次投票的结束时间。建议用since格式，但是合约并不检查，而是留给链下计票服务检查。

extra： 为额外的信息，比如放置一些投票相关的文档的hash。

投票发起人构建VoteMeta并发送到链上之后，公开这个vote meta cell的outpoint，对投票内容进行公示。

### 投票

用户需要创建一个vote cell，创建的交易构成如下：

1. vote meta cell 作为 celldeps。
2. output的typescript为投票合约，args是vote meta cell的outpoint的hash(blake160)。
3. output的data为要投的选项的index，因为要支持多选，所以采用bit位的方式设置。类型为u32，所以最多只支持32个选项。
4. witness里放如下结构，lock_script_hash是投票人的地址，smt proof是该投票人在整个白名单组成的SMT里的proof。
    
    ```
    table VoteProof {
        lock_script_hash: Bytes,
        smt_proof: Bytes,
    }
    ```
    

### 计票

根据typescript可以过滤出某次投票的所有cell。

其他内容合约都检查过了，只有投票有效期需要链下计票服务检查。

解析data，拿到用户的投票选项

汇总投票人和选项即可得到投票结果。

## 地址绑定

示例代码参见  [web5-components/address-bind](https://github.com/web5fans/web5-components/tree/dev/address-bind)

1. 生成如下结构的绑定信息。timestamp是unix timestamp，单位毫秒。

```jsx
table BindInfo {
    from: Script,
    to: Script,
    timestamp: Uint64,
}
```

1. 序列化该结构，并转为成十六进制字符串（带0x前缀）
2. 使用from对应的钱包（比如Neuron）的消息签名功能对该字符串进行签名，放到一起形成如下结构：

```jsx
table BindInfoWithSig {
    bind_info: BindInfo,
    sig: Bytes,
}
```

1. 使用to的地址发一个交易。交易结构为一个input，一个output，且lock相同（即自己给自己转账）。把 BindInfoWithSig 放到witness的inputType字段。
2. 链外indexer服务扫描交易，识别地址绑定交易，并验证签名和相关的有效性。
3. timestamp跟交易所在区块时间对比，确保timestamp在合理范围内（20分钟）。
3. 如果有效，将bind info记录到数据库，同一个from只能有一条有效记录，按上链顺序排列，更新的记录覆盖之前的记录。
4. 提供API让业务系统可以查询绑定关系。

## Community fund DAO场景完整业务流程

投票采用前面所述的合约，但是信息来源以及一些流程会和业务场景相关。

### 投票人准备

用户需要满足两个条件才能参与Community fund DAO的投票：

1. 在Nervos DAO中质押了CKB。
2. 注册Web5 DID并在Community fund DAO 1.1系统中登录过。

针对前一点，因为大部分人在Nervos DAO中质押CKB都是用的Neuron，而后续投票需要在线发交易，Neuron不方便操作。

所以使用前面的地址绑定方案，把Neuron地址绑定到Community fund DAO 1.1系统账号关联的CKB地址。

### 发起提案

1. 提案发起人在Community fund DAO 1.1系统中发一个提案（普通帖子）。
2. 讨论期内，其他用户可以对提案进行讨论回复，提案发起人可以对提案进行编辑
3. 讨论期结束，冻结提案。Community fund DAO 1.1后端服务保存当前提案内容和提案人的签名，此后提案不能修改（前端展示的提案内容改为后端服务保存的副本）。

### 发起投票

示例代码参见 [web5-components/vote/create-vote-meta](https://github.com/web5fans/web5-components/tree/dev/vote/create-vote-meta)

创建vote meta cell，详细内容参见前面的合约spec。这里只是补充一下投票发起人在Community fund DAO 1.1系统中发起投票时，需要提供的信息分别来自那里。

白名单相关：示例代码 [web5-components/vote/smt](https://github.com/web5fans/web5-components/tree/dev/vote/smt)

1. Community fund DAO 1.1系统自动收集系统用户集合，并过滤掉没有绑定或者没有质押CKB的用户，该用户集合即为投票的白名单。
2. 使用白名单用户的lock script hash组装成SMT，计算出smt_root_hash。
3. 同时提供API接口，可以让用户查询自己是否有投票权（是否在白名单内），如果有的话给出smt proof。

candidates 由Community fund DAO 1.1系统固定为（YES/NO/neither）。

start_time 和 end_time 由Community fund DAO 1.1系统自动设定。

extra 存放提案的hash。

### 用户投票

示例代码  [web5-components/vote/user-vote](https://github.com/web5fans/web5-components/tree/dev/vote/user-vote)

用户选择参与哪项投票，就选择对应的vote meta cell，typescript也就确定了。

投票给哪些项由用户自己选择。

smt_proof 由Community fund DAO 1.1系统自动根据投票用户的lock script hash获取。

### 计票

DAO场景特殊的一点是，用户发送交易创建vote cell之后，会马上再发起一个交易把它销毁掉，不长期占用用户的CKB。

即vote cell只要在历史中出现过就算。

允许用户多次投票，并修改投票选项，计票时以最后一次投票为准。

计票结果不包含权重，需要根据投票人权重（质押的CKB数量）再计算一次，才是最终的投票结果。
