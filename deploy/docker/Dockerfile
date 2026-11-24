FROM golang:1.26-alpine AS builder

ARG GOPROXY=https://proxy.golang.org,direct
ARG GOSUMDB=sum.golang.org
ENV GOPROXY=${GOPROXY} GOSUMDB=${GOSUMDB}

RUN apk add --no-cache gcc musl-dev

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
COPY migrations ./migrations

RUN mkdir -p /out/rootfs/var/lib/newspulse /out/rootfs/etc/newspulse \
    && touch /out/rootfs/var/lib/newspulse/.keep \
    && chown -R 10001:10001 /out/rootfs

RUN CGO_ENABLED=1 GOOS=linux go build -buildvcs=false \
    -tags "netgo osusergo" -trimpath \
    -ldflags "-s -w -linkmode external -extldflags \"-static\"" \
    -o /out/newspulse ./cmd/newspulse

FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder --chown=10001:10001 /out/rootfs/ /

COPY --from=builder /out/newspulse /newspulse

USER 10001:10001
WORKDIR /var/lib/newspulse
EXPOSE 8080
STOPSIGNAL SIGTERM
ENV TZ=Asia/Shanghai

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
    CMD ["/newspulse", "healthcheck"]

ENTRYPOINT ["/newspulse"]
CMD ["serve", "--config", "/etc/newspulse/config.yaml"]
