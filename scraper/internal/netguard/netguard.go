// Package netguard decides which network addresses the crawler may connect
// to on behalf of crawled content.
//
// The crawler fetches arbitrary internet URLs -- seeds, discovered links,
// sitemap entries, robots.txt Sitemap: lines and every redirect target --
// from inside a network that also holds PostgreSQL, Kafka, Temporal, S3, the
// Spark master and other internal services. Every such connection must go to
// a public address. The check is meant to run at dial time, on the IP that is
// actually being connected to (see Policy.Control), so neither DNS rebinding
// nor a redirect to an internal name can get around it.
package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"syscall"
)

// Policy is the address policy for crawler traffic. The zero value denies
// every non-public address.
type Policy struct {
	// AllowPrivate turns the check off. Only for tests (httptest servers
	// listen on 127.0.0.1) and local experiments against private hosts.
	AllowPrivate bool
}

// BlockedError reports a refused destination address.
type BlockedError struct {
	Addr   netip.Addr
	Reason string
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("refusing to connect to %s: %s address", e.Addr, e.Reason)
}

// IsBlocked reports whether err (or an error it wraps) is a *BlockedError.
func IsBlocked(err error) bool {
	var b *BlockedError
	return errors.As(err, &b)
}

// blockedPrefixes are special-purpose ranges (IANA IPv4/IPv6 special-purpose
// address registries) that are not covered by the netip.Addr predicates used
// in Reason, with the reason reported for them.
var blockedPrefixes = []struct {
	prefix netip.Prefix
	reason string
}{
	{netip.MustParsePrefix("0.0.0.0/8"), "this-network"},
	{netip.MustParsePrefix("100.64.0.0/10"), "carrier-grade NAT"},
	{netip.MustParsePrefix("192.0.0.0/24"), "IETF protocol assignment"},
	{netip.MustParsePrefix("192.0.2.0/24"), "documentation"},
	{netip.MustParsePrefix("192.88.99.0/24"), "6to4 relay anycast"},
	{netip.MustParsePrefix("198.18.0.0/15"), "benchmarking"},
	{netip.MustParsePrefix("198.51.100.0/24"), "documentation"},
	{netip.MustParsePrefix("203.0.113.0/24"), "documentation"},
	{netip.MustParsePrefix("240.0.0.0/4"), "reserved"}, // includes 255.255.255.255
	{netip.MustParsePrefix("::/96"), "IPv4-compatible"},
	{netip.MustParsePrefix("64:ff9b:1::/48"), "local-use NAT64"},
	{netip.MustParsePrefix("100::/64"), "discard-only"},
	{netip.MustParsePrefix("2001::/32"), "Teredo"},
	{netip.MustParsePrefix("2001:2::/48"), "benchmarking"},
	{netip.MustParsePrefix("2001:10::/28"), "ORCHID"},
	{netip.MustParsePrefix("2001:20::/28"), "ORCHIDv2"},
	{netip.MustParsePrefix("2001:db8::/32"), "documentation"},
	{netip.MustParsePrefix("3fff::/20"), "documentation"},
	{netip.MustParsePrefix("5f00::/16"), "SRv6 SID"},
	{netip.MustParsePrefix("fec0::/10"), "site-local"},
}

var (
	nat64Prefix = netip.MustParsePrefix("64:ff9b::/96")
	sixToFour   = netip.MustParsePrefix("2002::/16")
)

// Reason returns why addr is not a public unicast address, or "" if it is.
// IPv4 addresses embedded in IPv6 (IPv4-mapped, NAT64, 6to4) are judged by
// the IPv4 address they carry.
func Reason(addr netip.Addr) string {
	if !addr.IsValid() {
		return "invalid"
	}
	addr = addr.Unmap()
	switch {
	case addr.IsUnspecified():
		return "unspecified"
	case addr.IsLoopback():
		return "loopback"
	case addr.IsPrivate():
		return "private"
	case addr.IsLinkLocalUnicast(), addr.IsLinkLocalMulticast():
		return "link-local"
	case addr.IsMulticast(), addr.IsInterfaceLocalMulticast():
		return "multicast"
	}
	if addr.Is6() {
		if nat64Prefix.Contains(addr) {
			b := addr.As16()
			if r := Reason(netip.AddrFrom4([4]byte{b[12], b[13], b[14], b[15]})); r != "" {
				return "NAT64-embedded " + r
			}
			return ""
		}
		if sixToFour.Contains(addr) {
			b := addr.As16()
			if r := Reason(netip.AddrFrom4([4]byte{b[2], b[3], b[4], b[5]})); r != "" {
				return "6to4-embedded " + r
			}
			return ""
		}
	}
	for _, p := range blockedPrefixes {
		if p.prefix.Contains(addr) {
			return p.reason
		}
	}
	if !addr.IsGlobalUnicast() {
		return "non-global"
	}
	return ""
}

// Check returns a *BlockedError when p does not allow connecting to addr.
func (p Policy) Check(addr netip.Addr) error {
	if p.AllowPrivate {
		return nil
	}
	if r := Reason(addr); r != "" {
		return &BlockedError{Addr: addr.WithZone(""), Reason: r}
	}
	return nil
}

// Control is a net.Dialer Control function enforcing p. The dialer calls it
// after name resolution with the literal "ip:port" it is about to connect
// to, for every address it tries -- so it also covers redirects (a new
// connection) and hosts whose DNS answer changed since an earlier check.
func (p Policy) Control(_, address string, _ syscall.RawConn) error {
	if p.AllowPrivate {
		return nil
	}
	ap, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("refusing to connect to unparsable address %q: %w", address, err)
	}
	return p.Check(ap.Addr())
}

// Resolver is the subset of *net.Resolver the lookup helpers need.
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

// resolve parses host as an IP literal or looks it up.
func resolve(ctx context.Context, r Resolver, host string) ([]netip.Addr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []netip.Addr{ip}, nil
	}
	addrs, err := r.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, &net.DNSError{Err: "no addresses", Name: host, IsNotFound: true}
	}
	return addrs, nil
}

// LookupAllowed resolves host (or parses it, if it is an IP literal) and
// returns the addresses p allows, failing with a *BlockedError when none is
// allowed. For dialers that then connect to one of the returned addresses
// themselves.
func (p Policy) LookupAllowed(ctx context.Context, r Resolver, host string) ([]netip.Addr, error) {
	addrs, err := resolve(ctx, r, host)
	if err != nil {
		return nil, err
	}
	allowed := addrs[:0:0]
	var firstErr error
	for _, a := range addrs {
		if err := p.Check(a); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		allowed = append(allowed, a)
	}
	if len(allowed) == 0 {
		return nil, firstErr
	}
	return allowed, nil
}

// CheckHost resolves host and fails with a *BlockedError if any of its
// addresses is refused. It is a pre-flight check for clients that resolve
// and dial on their own (headless Chrome), which may pick any address of
// the answer -- and may resolve again later (DNS rebinding), so a passing
// check is weaker than Control's dial-time check.
func (p Policy) CheckHost(ctx context.Context, r Resolver, host string) error {
	if p.AllowPrivate {
		return nil
	}
	addrs, err := resolve(ctx, r, host)
	if err != nil {
		return err
	}
	for _, a := range addrs {
		if err := p.Check(a); err != nil {
			return err
		}
	}
	return nil
}
