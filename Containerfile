FROM golang:1.21.6-bookworm as build

ENV CGO_ENABLED=0 \
	GOOS=linux \
	GOFLAGS="-tags=moderncsqlite"

WORKDIR /build

COPY . /build

RUN  make soju

FROM scratch
COPY --from=build /build/soju /app/soju
COPY --from=build /build/sojuctl /app/sojuctl
COPY --from=build /build/sojudb /app/sojudb
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

CMD ["/app/soju"]
