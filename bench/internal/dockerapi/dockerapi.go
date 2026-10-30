// Package dockerapi is the little of Docker that the benchmarks need: the bytes that a container
// has written to block devices (from its cgroup, or from the Docker API when the cgroup cannot be
// read), and the restart of a container. The API is reached over the unix socket of the daemon,
// which docker-compose.yml mounts into the harness; without it everything here says so, and the
// workloads record the figure as unavailable instead of guessing.
package dockerapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// DefaultSocket is where the daemon listens, and where compose mounts it into the harness.
const DefaultSocket = "/var/run/docker.sock"

// ErrUnavailable is returned when the figure cannot be had in any of the ways that were tried.
var ErrUnavailable = errors.New("unavailable")

// Client talks to the Docker API over a unix socket.
type Client struct {
	socket string
	http   *http.Client
}

// New returns a client of the daemon on the socket; an empty one is DefaultSocket. It does not
// connect: Available says whether the daemon answers.
func New(socket string) *Client {
	if socket == "" {
		socket = DefaultSocket
	}
	return &Client{
		socket: socket,
		http: &http.Client{
			Transport: &http.Transport{
				DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
					var d net.Dialer
					return d.DialContext(ctx, "unix", socket)
				},
				DisableKeepAlives: true,
			},
		},
	}
}

// Available reports whether the daemon answers on the socket.
func (c *Client) Available(ctx context.Context) bool {
	if _, err := os.Stat(c.socket); err != nil {
		return false
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	body, err := c.do(ctx, http.MethodGet, "/_ping", nil)
	return err == nil && strings.TrimSpace(string(body)) == "OK"
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values) ([]byte, error) {
	u := "http://docker" + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("docker: %s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(body)))
	}
	return body, nil
}

// WriteBytes returns the bytes that the container has written to block devices since it started,
// as the daemon reports them (blkio_stats of the stats of the container).
func (c *Client) WriteBytes(ctx context.Context, container string) (int64, error) {
	body, err := c.do(ctx, http.MethodGet, "/containers/"+url.PathEscape(container)+"/stats",
		url.Values{"stream": {"false"}, "one-shot": {"true"}})
	if err != nil {
		return 0, err
	}
	return ParseStatsWrites(body)
}

// ParseStatsWrites reads the bytes written from the JSON of the stats of a container. A container
// whose daemon has no block I/O figures (the controller is not on) is ErrUnavailable.
func ParseStatsWrites(body []byte) (int64, error) {
	var stats struct {
		Blkio struct {
			Bytes []struct {
				Op    string `json:"op"`
				Value int64  `json:"value"`
			} `json:"io_service_bytes_recursive"`
		} `json:"blkio_stats"`
	}
	if err := json.Unmarshal(body, &stats); err != nil {
		return 0, fmt.Errorf("docker: stats: %w", err)
	}
	if len(stats.Blkio.Bytes) == 0 {
		return 0, fmt.Errorf("docker: the stats have no block I/O figures: %w", ErrUnavailable)
	}
	var total int64
	for _, b := range stats.Blkio.Bytes {
		if strings.EqualFold(b.Op, "write") {
			total += b.Value
		}
	}
	return total, nil
}

// Restart restarts a container and returns when the daemon has started it again; the process in
// it is stopped with a signal first and killed after the timeout.
func (c *Client) Restart(ctx context.Context, container string, timeout time.Duration) error {
	_, err := c.do(ctx, http.MethodPost, "/containers/"+url.PathEscape(container)+"/restart",
		url.Values{"t": {strconv.Itoa(int(timeout.Seconds()))}})
	return err
}

// ParseIOStat sums the bytes written of every device in the text of the io.stat file of a cgroup
// (v2): lines like "259:0 rbytes=1 wbytes=2 rios=3 wios=4 dbytes=0 dios=0". A file with no line
// is ErrUnavailable: the io controller is not on for the cgroup.
func ParseIOStat(text string) (int64, error) {
	var total int64
	lines := 0
	for _, line := range strings.Split(text, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		lines++
		for _, f := range fields[1:] {
			if v, ok := strings.CutPrefix(f, "wbytes="); ok {
				n, err := strconv.ParseInt(v, 10, 64)
				if err != nil {
					return 0, fmt.Errorf("cgroup io.stat: %w", err)
				}
				total += n
			}
		}
	}
	if lines == 0 {
		return 0, fmt.Errorf("cgroup io.stat has no device: %w", ErrUnavailable)
	}
	return total, nil
}

// CgroupWriteBytes reads the bytes written to block devices by the cgroup of this process from
// its io.stat file; an empty path is the one of the container.
func CgroupWriteBytes(path string) (int64, error) {
	if path == "" {
		path = "/sys/fs/cgroup/io.stat"
	}
	bz, err := os.ReadFile(path)
	if err != nil {
		return 0, fmt.Errorf("%w: %v", ErrUnavailable, err)
	}
	return ParseIOStat(string(bz))
}

// Meter reads the bytes that a container has written, the first way that works.
type Meter struct {
	// Source says where the figures come from: "cgroup", "docker-api" or "unavailable".
	Source string
	read   func(ctx context.Context) (int64, error)
}

// Read returns the bytes written so far, or ErrUnavailable.
func (m *Meter) Read(ctx context.Context) (int64, error) {
	if m == nil || m.read == nil {
		return 0, ErrUnavailable
	}
	return m.read(ctx)
}

// Available reports whether the meter has a source.
func (m *Meter) Available() bool { return m != nil && m.read != nil }

// OwnMeter measures the container that the process runs in: the cgroup of the container first,
// and the Docker API second, for the container with the name (or the id, which a container has
// as its host name).
func OwnMeter(ctx context.Context, c *Client, self string) *Meter {
	if _, err := CgroupWriteBytes(""); err == nil {
		return &Meter{Source: "cgroup", read: func(context.Context) (int64, error) { return CgroupWriteBytes("") }}
	}
	if self == "" {
		self, _ = os.Hostname()
	}
	return apiMeter(ctx, c, self)
}

// ContainerMeter measures another container, which only the Docker API can do.
func ContainerMeter(ctx context.Context, c *Client, container string) *Meter {
	return apiMeter(ctx, c, container)
}

func apiMeter(ctx context.Context, c *Client, container string) *Meter {
	if c == nil || container == "" || !c.Available(ctx) {
		return &Meter{Source: "unavailable"}
	}
	read := func(ctx context.Context) (int64, error) { return c.WriteBytes(ctx, container) }
	if _, err := read(ctx); err != nil {
		return &Meter{Source: "unavailable"}
	}
	return &Meter{Source: "docker-api", read: read}
}
