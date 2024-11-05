# hsmq
Horizontal Scalable Message Queue Broker

[![rustc](https://img.shields.io/badge/Rustc-1.79.0-blue?logo=rust)](https://www.rust-lang.org/)
[![Actions Status](https://github.com/aamalev/hsmq/actions/workflows/fmt.yml/badge.svg)](https://github.com/aamalev/hsmq/actions)
[![Actions Status](https://github.com/aamalev/hsmq/actions/workflows/clippy.yml/badge.svg)](https://doc.rust-lang.org/stable/clippy/)
[![Actions Status](https://github.com/aamalev/hsmq/actions/workflows/tests.yml/badge.svg)](https://github.com/aamalev/hsmq/actions)

## Features

- [x] Interface
    - [x] gRPC
    - [ ] WebSocket
- [x] Configurable service
- [x] Graceful shutdown
- [x] Prometheus metrics
- [x] Consul
- [x] Sentry
- [x] Vault
- [x] Declare queue params
- [x] Console client hsmq-cli
    - [x] Publish
    - [x] Publish QoS0
    - [x] Subscribe to a queue
- [x] Queue
    - [x] Bind on static topics
    - [x] In memory buffer
    - [x] Graceful serve consumers on shutdown
    - [x] Metrics
    - [x] Limit
    - [x] Ack
    - [ ] TTL for messages
    - [ ] Dynamic queue with ttl
- [x] Auth
    - [x] Tokens
    - [x] JWT
- [ ] Cluster
    - [ ] Static routes
    - [ ] DNS routes
    - [ ] Broadcast routes
    - [ ] Hot replication queue on shutdown


## Message life cycle

Message contains:

    - data
    - headers
    - topic

After publication, messages are filtered by topic.
If the topic satisfies the condition, the message is saved in the queue.

## Queues

### InMemory

```toml
[[queues]]
type = "InMemory"
name = "b"
limit = 99  # oldest messages will be drop with metric label "drop-limit"
ack_timeout = 1  # in seconds, or use { m = 3 } to specify 3 minutes
topics = [
    "b.a",  # all messages with topic "b.a" will be kept to queue "b"
]
```


## Auth

### JWT

Your secrets must be kept to config:

```toml
[auth.jwt]
secrets = [
    "inline secret",
    { env = "JWT_SECRET" },  # secret from environment variable
    { name = "old_secret_2", env = "OLD_JWT_2" },  # name for metrics
    { name = "old_secret_1", env = "OLD_JWT_1", disable = true },
]
```

You must specify a header in each request:

    authorization: Bearer {jwt-token}

Required claims with token:

* exp - deadline unix timestamp
* sub - username


### Tokens

Your tokens must be kept to config for each user:

```toml
[users.my_username]
tokens = [
    "inline personal token",
    { env = "TOKEN" },  # token from environment variable
    { env = "TOKEN_1", disable = true },
]
```

You must specify a header in each request:

    authorization: Bearer {token}


### Vault

For enable vault add to config block [vault]

```toml
[vault]
```

```toml
[vault]  # enable vault
uri = "http://localhost:8200"  # optional
auth.jwt.env = "VAULT_JWT_TOKEN"  # optional
token = { env = "VAULT_TOKEN", file = "/etc/vault-token.json", json_field = "jwt" }  # optional
ca_cert = "/etc/ca.pem"  # optional

[auth.jwt]
secrets = [
    { vault_path = "project/hsmq/jwt_1", json_field = "token", vault_mount = "secret" },  # field from json from kv2
    { vault_path = "project/hsmq/jwt_2", vault_kv_version = 1, json_field = "token" },  # token from kv1
]
```
