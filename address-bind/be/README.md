# address bind backend

- [x] verifier
- [x] scanner
- [x] api

## verfiy

```
$ address-bind-be verify -t 0x024bf0f881b020e91384c2b83258cac99fcc93c049dc8e2b138c90ef7bca7ce3
tx 024bf0f881b020e91384c2b83258cac99fcc93c049dc8e2b138c90ef7bca7ce3 has valid bind info, bind_type: bind, from: ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah, to: ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah, timestamp: 1760432079687
```

## indexer

```
$ address-bind-be indexer -s 18829898
current_height: 18829898
from: ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah, to: ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah, timestamp: 1760432079687
current_height: 18829899
```

## api

```
API documentation including:

1.
   /health endpoint
   
   - Description: Check the health status of the API server
   - Response: JSON object with a single property "status" set to "OK" if the server is running

2.
   /by_from/:from endpoint
   
   - Description: Query binding information by from address
   - Parameters: from - The source address to query
   - Response: Array containing target address (to) and height, tx_index for each binding record
3.
   /by_to/:to endpoint
   
   - Description: Query binding information by to address
   - Parameters: to - The target address to query
   - Response: Array containing source address (from) and height, tx_index for each binding record
   - Note: For each from address, only returns the record with the latest height, tx_index

4.
  /by_to_at_height/:to/:height endpoint
   
   - Description: Query binding information by to address at a specific height
   - Parameters: to - The target address to query, height - The height to query
   - Response: Array containing source address (from) and height, tx_index for each binding record at the specified height
   - Note: For each from address, only returns the record with the latest height, tx_index
```

```
$ curl http://localhost:9533/by_from/ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ahah
[{"height":18977278, "tx_index":1, "to":"ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah"}]

$ curl -vv http://localhost:9533/by_to/ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah
[{"height":18977278, "tx_index":1, "from":"ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah"}]

$ curl -vv http://localhost:9533/by_to_at_height/ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah/18977278
[{"height":18977278, "tx_index":1, "from":"ckt1qzda0cr08m85hc8jlnfp3zer7xulejywt49kt2rr0vthywaa50xwsqwu8lmjcalepgp5k6d4j0mtxwww68v9m6qz0q8ah"}]
```
