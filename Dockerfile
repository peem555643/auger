# Production image for Auger.
#
# Two stages: a full Rust toolchain compiles, and a slim Debian runs the
# resulting binary. The runtime layer carries no compiler, no cargo registry
# and no source — roughly 90 MB against ~1.5 GB for the builder.
#
#   docker build -t auger:0.1.0 .

ARG RUST_VERSION=1.95
ARG DEBIAN_RELEASE=bookworm

FROM rust:${RUST_VERSION}-${DEBIAN_RELEASE} AS builder
WORKDIR /build

# Dependencies are compiled in their own layer, against a placeholder main.
# DataFusion and Arrow are the bulk of the build and change only when
# Cargo.lock does; without this split, every edit to src/ recompiles them.
COPY Cargo.toml Cargo.lock ./
RUN mkdir src \
 && echo 'fn main() {}' > src/main.rs \
 && cargo build --release --locked \
 && rm -rf src

COPY src ./src
# COPY carries the context's mtimes over, which can land *older* than the
# placeholder build; touch guarantees cargo sees the real source as newer.
RUN touch src/main.rs \
 && cargo build --release --locked \
 && strip target/release/auger

FROM debian:${DEBIAN_RELEASE}-slim AS runtime

# Mongo's TLS goes through rustls with roots compiled in, so ca-certificates is
# not needed for the driver — it is here for anything that consults the system
# store, and costs a few hundred KB.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Fixed uid so a bind-mounted catalog cache can be chowned from the host.
RUN useradd --system --uid 10001 --create-home --home-dir /var/lib/auger \
            --shell /usr/sbin/nologin auger

COPY --from=builder /build/target/release/auger /usr/local/bin/auger

USER auger
WORKDIR /var/lib/auger
EXPOSE 5433

# Inside a container the loopback default would make the port unreachable even
# when published. Every one of these is overridable per `docker run -e`.
ENV AUGER_LISTEN=0.0.0.0:5433 \
    AUGER_CATALOG_CACHE=/var/lib/auger/catalog.json \
    AUGER_LOG=info

# Flags passed to `docker run` replace CMD, not ENTRYPOINT — so an ad-hoc
# invocation must repeat the config path:
#   docker run --rm auger:0.1.0 --config /etc/auger/auger.toml --describe
ENTRYPOINT ["/usr/local/bin/auger"]
CMD ["--config", "/etc/auger/auger.toml"]
