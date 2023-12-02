FROM golang:1.21 as builder

WORKDIR /src

COPY . .

RUN go build

FROM debian:12

RUN apt-get update && apt-get install -y ca-certificates && apt-get clean

COPY  --from=builder /src/main /otlptest

CMD /otlptest