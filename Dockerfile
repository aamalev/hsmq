FROM rust:1.79

RUN apt update
RUN apt install -y protobuf-compiler

WORKDIR /usr/src/hsmq
COPY Cargo.toml build.rs ./
COPY src src
COPY proto proto

RUN cargo install --path .

COPY hsmq.toml /etc/

CMD ["hsmq", "--config", "/etc/hsmq.toml"]
