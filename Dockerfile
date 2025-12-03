FROM golang:1.21-alpine

WORKDIR /app

# Copy and build
COPY . .
RUN go mod download && \
    go build -o bin/driver ./driver && \
    go build -o bin/worker ./worker && \
    go build -o bin/client ./client

# Create directories
RUN mkdir -p data logs output

EXPOSE 8080 9000 9100 9101 9102 9103

CMD ["./bin/driver"]