# hsmq
Horizontal Scalable Message Queue Broker

## Features

- [x] Interface
    - [x] gRPC
    - [ ] WebSocket
- [x] Configurable service
- [x] Graceful shutdown
- [x] Prometheus metrics
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
