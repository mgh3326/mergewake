# syntax=docker/dockerfile:1

# The mergewake daemon as a container. The image carries only mergewake and
# the tools it shells out to (git, sh, ssh); runtug, panewire and handoffkeep,
# their configuration and every credential are mounted at run time — see
# docs/container.md. Nothing secret is baked in.
FROM --platform=$BUILDPLATFORM golang:1.27-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG TARGETOS TARGETARCH
ARG VERSION=dev
ARG COMMIT=unknown
# The build context has no .git (.dockerignore); the commit is stamped through
# ldflags instead and `mergewake commit` prints it.
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -buildvcs=false \
    -ldflags="-s -w -X main.version=${VERSION} -X main.commit=${COMMIT}" \
    -o /out/mergewake ./cmd/mergewake

# Runtime: debian-slim rather than distroless because the daemon runs git
# (review checkouts, reap), sh (reap probes) and ssh (remote hosts, runtug
# placement). Nonroot uid 65532 with its own HOME, so the default config path
# ~/.config/mergewake/config.yaml and state dir resolve under it.
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates git openssh-client \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 65532 --user-group --create-home --home-dir /home/mergewake --shell /usr/sbin/nologin mergewake
COPY --from=build /out/mergewake /usr/local/bin/mergewake
USER 65532:65532
ENV HOME=/home/mergewake
WORKDIR /home/mergewake
ENTRYPOINT ["/usr/local/bin/mergewake"]
CMD ["daemon"]
