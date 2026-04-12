FROM golang:1.23-alpine AS builder
RUN CGO_ENABLED=0 go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest 2>/dev/null; true
# Build a tiny HTTP fetcher that downloads pprof profiles
RUN cat > /tmp/fetch_pprof.go <<'EOF'
package main

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: fetch_pprof <url> [output_file]")
		os.Exit(1)
	}
	url := os.Args[1]
	out := "/tmp/cpu.prof"
	if len(os.Args) > 2 {
		out = os.Args[2]
	}
	client := &http.Client{Timeout: 120 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "GET %s: %v\n", url, err)
		os.Exit(1)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "HTTP %d: %s\n", resp.StatusCode, string(body))
		os.Exit(1)
	}
	f, err := os.Create(out)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create %s: %v\n", out, err)
		os.Exit(1)
	}
	n, _ := io.Copy(f, resp.Body)
	f.Close()
	fmt.Fprintf(os.Stderr, "wrote %d bytes to %s\n", n, out)
}
EOF
RUN cd /tmp && CGO_ENABLED=0 go build -o /usr/local/bin/fetch_pprof fetch_pprof.go

FROM minio/minio:latest
COPY --from=builder /usr/local/bin/fetch_pprof /usr/local/bin/fetch_pprof
