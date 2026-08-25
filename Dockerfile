# syntax=docker/dockerfile:1

# The stepd single binary: server, console, migrations and operations.
#
# Two stages. The build needs no database — there are no `sqlx::query!` macros
# in this workspace, and the migrations are compiled in by `sqlx::migrate!`, so
# `stepd migrate` carries its own SQL and the runtime image needs no source.

# ----------------------------------------------------------------- build
#
# Latest stable 1.x, which is what CI resolves `dtolnay/rust-toolchain@stable`
# to. NOT the workspace's `rust-version = "1.85"`: that claim is currently
# false, because Cargo.lock pins icu_properties_data 2.3.0 and it requires
# 1.88. Pinning the declared MSRV here fails the build outright.
FROM rust:1-bookworm AS build
WORKDIR /src

# Three workspaces, in the repository's own layout. `engine/rust` path-depends
# on `../../spec/rust` (the protocol) and `../../sdk/rust` (the conformance
# battery's reference app), so the relative arrangement has to be preserved
# here — a flattened copy fails to resolve rather than building less.
COPY spec/rust/   spec/rust/
COPY sdk/rust/    sdk/rust/
COPY engine/rust/ engine/rust/
WORKDIR /src/engine/rust

# Cache mounts rather than a dependency-only pre-build: the registry and the
# target directory survive between builds, and the binary is copied out inside
# the same RUN because a cache mount is not present in the resulting layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/engine/rust/target \
    cargo build --release -p stepd-cli && \
    cp target/release/stepd /usr/local/bin/stepd

# ----------------------------------------------------------------- runtime
FROM debian:bookworm-slim AS runtime

# ca-certificates: the dispatcher calls application endpoints over TLS.
# curl: the compose healthcheck hits /v1/health, and an image whose health
# cannot be checked from inside makes `depends_on: service_healthy` unusable.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*

# Non-root. The blob root is the only path written at runtime.
RUN useradd --system --create-home --uid 10001 stepd \
 && mkdir -p /var/lib/stepd/blobs \
 && chown -R stepd:stepd /var/lib/stepd

COPY --from=build /usr/local/bin/stepd /usr/local/bin/stepd

USER stepd
WORKDIR /var/lib/stepd
EXPOSE 8080

# No shell wrapper: the server drains in-flight attempts on SIGTERM, and a shell
# as PID 1 would swallow the signal and turn every deploy into a reclaim storm.
ENTRYPOINT ["/usr/local/bin/stepd"]
CMD ["serve"]
