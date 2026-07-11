# dlock

`dlock` is a distributed lock utility, similar in spirit to
[`flock`](https://man.archlinux.org/man/flock.1), backed by
[etcd](https://etcd.io/). It can prevent multiple instances of a service
running at the same time while run another instance when the running one is
down, e.g. good for Telegram bots.

## Building

Install Rust and run:

```sh
cargo build --release
```

You'll find the built binary at `target/release/dlock`.

## Usage

```
dlock --config <PATH> --nodename <NAME> -- <COMMAND> [ARGS...]
```

- `--config`: path to the configuration file.
- `--nodename`: a unique name identifying this participant in the election.
- The remaining arguments form the command to run while holding the lock. A
  `--` separator is recommended.

## Configuration

Configuration is a TOML file. See
[`config.toml.example`](config.toml.example) for a complete example.

| Field      | Type    | Required | Description                                            |
|------------|---------|----------|--------------------------------------------------------|
| `lockname` | string  | yes      | The lock to contend for.                               |
| `endpoints`| array   | yes      | A list of etcd endpoint URLs.                          |
| `ttl`      | integer | no       | Lease time-to-live, in seconds. Defaults to `5`.       |
| `cert`     | table   | no       | TLS configuration. Omit to connect without TLS.        |

The optional `cert` table contains `cert`, `key`, and `ca` paths to PEM files
for mutual TLS.
