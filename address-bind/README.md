# address-bind

bind address in a offline wallet(for example Neuron) to address in web wallet.

## steps

1. generate bind info message. timestamp is unix timestamp in ms.
    ```
    table BindInfo {
        to: Script,
        timestamp: Uint64,
    }
    ```
2. serialize message and convert to hex string(with prefix `0x`).
3. sign hex string with Neuron.
4. compose message with signature.
    ```
    table BindInfoWithSig {
        bind_info: BindInfo,
        sig: Bytes,
    }
    ```
5. use web wallet transfer some ckb to itself. put BindInfoWithSig in witness.
6. backend scan ckb tx, verify signature. if valid, record bind relationship in database. for same from address, bind info with later timestamp will update bind relationship.
7. frontend query bind relationship from backend. if bind relationship exists, show bind info. if not, show bind form.

## frontend

generate bind info:
1. generate bind info message.
2. serialize message and convert to hex string(with prefix `0x`).
3. print this hex string.

bind address:
1. connect web wallet.
2. user input bind info message.
3. user input signature.
4. compose BindInfoWithSig.
5. use web wallet transfer some ckb to itself with BindInfoWithSig in witness.

## backend

scanner:
1. scan ckb tx. filter bind address tx.
2. for each bind address tx, verify signature in BindInfoWithSig, and check timestamp is around current block timestamp(within 20min).
3. if valid, record bind relationship in database. for same from address, bind info with later timestamp will update bind relationship.

api:
1. query bind relationship by from address.
2. query bind relationship by to address.


## unbind

for unbind, reuse BindInfo.
to script of the address to unbind, sig is empty.

backend will check if the bind relationship exists.
if exists, insert bind record with from address and black hole address.
