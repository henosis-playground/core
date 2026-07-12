FROM rust:1.96-bookworm AS builder

RUN apt-get update \
    && apt-get install --yes --no-install-recommends \
        clang \
        cmake \
        git \
        libpq-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY . .
RUN cargo build --locked --release -p henosis-core-server

FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install --yes --no-install-recommends ca-certificates curl libpq5 \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home --uid 1000 henosis

COPY --from=builder /src/target/release/henosis-core-server /usr/local/bin/henosis-core-server

USER henosis
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/henosis-core-server"]
