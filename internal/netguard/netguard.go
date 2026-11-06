// Package netguard limits where the harness may connect on behalf of a
// tenant. URLs controlled by a tenant admin could otherwise make harness
// hosts call loopback services or the cloud metadata endpoint (SSRF).
package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"syscall"
	"time"
)

// ErrBlocked marks a destination refused by the policy. Callers use it to
// distinguish a permanent policy denial from a transient network failure.
var ErrBlocked = errors.New("destination blocked by network policy")

// NetPolicy is a set of network destinations that are allowed in addition to
// the default block list. By default loopback, link-local, unspecified and
// multicast addresses are refused. Cloud metadata addresses are refused even
// when an exception covers them.
//
// The check runs on the resolved address at connect time, so a host name
// that later resolves elsewhere (DNS rebinding) is still caught.
type NetPolicy struct {
	Allow []netip.Prefix
}

var metadataAddrs = []netip.Addr{
	netip.MustParseAddr("169.254.169.254"),
	netip.MustParseAddr("100.100.100.200"),
	netip.MustParseAddr("fd00:ec2::254"),
}

var metadataHosts = map[string]bool{"metadata.google.internal": true, "metadata": true}

// ParseNetworks parses a comma-separated list of CIDRs or single addresses.
func ParseNetworks(v string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, f := range strings.Split(v, ",") {
		f = strings.TrimSpace(f)
		if f == "" {
			continue
		}
		if !strings.Contains(f, "/") {
			a, err := netip.ParseAddr(f)
			if err != nil {
				return nil, fmt.Errorf("invalid network %q", f)
			}
			out = append(out, netip.PrefixFrom(a, a.BitLen()))
			continue
		}
		p, err := netip.ParsePrefix(f)
		if err != nil {
			return nil, fmt.Errorf("invalid network %q", f)
		}
		out = append(out, p.Masked())
	}
	return out, nil
}

// CheckAddr reports whether a connection to ip is allowed.
func (p NetPolicy) CheckAddr(ip netip.Addr) error {
	ip = ip.Unmap()
	for _, m := range metadataAddrs {
		if ip == m {
			return fmt.Errorf("%w: %s is a cloud metadata address", ErrBlocked, ip)
		}
	}
	for _, a := range p.Allow {
		if a.Contains(ip) {
			return nil
		}
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() ||
		ip.IsMulticast() || ip.IsInterfaceLocalMulticast() {
		return fmt.Errorf("%w: %s is not allowed (HARNESS_EXTENSIONS_ALLOW_NETWORKS adds exceptions)", ErrBlocked, ip)
	}
	return nil
}

// CheckHost rejects URLs whose host is known to be refused, so an operator
// sees the error when saving instead of at the first call.
func (p NetPolicy) CheckHost(u *url.URL) error {
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	if metadataHosts[host] {
		return fmt.Errorf("destination %s is a cloud metadata address", host)
	}
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		host = "127.0.0.1"
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		return p.CheckAddr(ip)
	}
	return nil
}

func (p NetPolicy) control(_, address string, _ syscall.RawConn) error {
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("cannot check destination %q: %w", address, err)
	}
	return p.CheckAddr(ap.Addr())
}

// Transport returns an HTTP transport that enforces the policy on every
// connection. Proxies are disabled: through a proxy the dialed address would
// be the proxy's, not the destination's.
func (p NetPolicy) Transport() *http.Transport {
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second, Control: p.control}
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		return d.DialContext(ctx, network, addr)
	}
	return t
}

// Client returns an HTTP client using Transport.
func (p NetPolicy) Client(timeout time.Duration) *http.Client {
	return &http.Client{Transport: p.Transport(), Timeout: timeout}
}
