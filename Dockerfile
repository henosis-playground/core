FROM rust:1.96-bookworm AS build

WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml rustfmt.toml ./
COPY crates crates
COPY services services
COPY tests tests
COPY proto proto
RUN cargo build --release --locked -p henosis-core-server

FROM node:22-bookworm-slim AS runtime

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        git \
    && npm install --global wrangler@4.82.2 \
    && rm -rf /var/lib/apt/lists/* /root/.npm

COPY --from=build /app/target/release/henosis-core-server /usr/local/bin/henosis-core-server

EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/henosis-core-server"]
