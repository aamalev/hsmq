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
    - [x] Tokens from config
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
