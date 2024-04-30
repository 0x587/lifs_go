FROM golang:1.22 AS base

RUN apt update && apt install -y libfuse2 fuse3

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

FROM base AS test

COPY . ./

CMD ["go", "test", "./..."]

