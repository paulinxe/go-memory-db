FROM golang:1.26-alpine

# Install build tools and utilities
RUN apk add --no-cache git gcc musl-dev make bash libstdc++ libc6-compat

# Ensure Go is in PATH (must be set early)
ENV PATH="/usr/local/go/bin:${PATH}"

# Add Go to PATH in profile.d for all shells (Alpine Linux standard)
RUN echo 'export PATH="/usr/local/go/bin:$PATH"' > /etc/profile.d/go.sh && \
    chmod +x /etc/profile.d/go.sh

# Install golangci-lint with same Go as image (so version matches go.mod).
# Install to /usr/local/bin so it's found in devcontainer terminals (they often use a minimal PATH without /go/bin).
ENV PATH="/go/bin:${PATH}"
RUN go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest && \
    cp /go/bin/golangci-lint /usr/local/bin/

WORKDIR /app

# Copy go mod files first (for caching)
#COPY go.mod go.sum ./
#RUN go mod download

# Copy the rest of the code
COPY . .

# Create user matching host IDs
ARG UID=1000 # Default value if not provided
ARG GID=1000 # Default value if not provided
RUN addgroup -g ${GID} appgroup && \
    adduser -u ${UID} -G appgroup -D -g "" appuser

RUN chown appuser:appgroup /app

# Ensure appuser can write to .cache (golangci-lint/goimports, gopls, module downloads, etc.)
RUN mkdir -p /home/appuser/.cache/go-mod && chown -R appuser:appgroup /home/appuser

ENV GOCACHE=/home/appuser/.cache/go-cache
ENV GOMODCACHE=/home/appuser/.cache/go-mod

### Development only
RUN go install golang.org/x/tools/gopls@latest
RUN chown -R appuser:appgroup /home/appuser/.cache/go-cache /home/appuser/.cache/go-mod
### End of development only

USER appuser

CMD ["tail", "-f", "/dev/null"]
