# unifykv

### Development

```shell
# format
cargo fmt

# check
cargo check

# lint
cargo clippy

# compile
cargo build

# all of the above
bin/test.sh
```

### Running

```shell
# setup .env
cp .env.sample .env

# general key and cert pem files
bin/gen-ssl-cert.sh

# run
cargo run

# in another terminal
# (requires installing `ncat`)
ncat --ssl localhost 7719
```

