FROM golang:1.11.1 AS builder

COPY ./scheduler $GOPATH/src/github.com/fbeltrao/aksjobscheduler/scheduler
COPY ./schedulerapi $GOPATH/src/github.com/fbeltrao/aksjobscheduler/schedulerapi

# Copy the code from the host and compile it
WORKDIR $GOPATH/src/github.com/fbeltrao/aksjobscheduler/server
COPY ./server/. .

RUN go get -d -v
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix nocgo -o /app .

FROM alpine:3.7
RUN apk add --update \
  ca-certificates
COPY --from=builder /app ./
ENTRYPOINT ["./app"]