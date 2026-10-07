package netguard

import (
	"context"
	"errors"
	"net/netip"
	"testing"
)

func TestReason(t *testing.T) {
	blocked := []string{
		"0.0.0.0", "0.1.2.3", "10.0.0.1", "10.255.255.255", "100.64.0.1", "100.127.255.254",
		"127.0.0.1", "127.255.0.1", "169.254.169.254", "169.254.0.1", "172.16.0.1", "172.31.255.255",
		"192.0.0.8", "192.0.2.10", "192.88.99.1", "192.168.1.1", "198.18.0.1", "198.19.255.255",
		"198.51.100.7", "203.0.113.9", "224.0.0.1", "239.255.255.250", "240.0.0.1", "255.255.255.255",
		"::", "::1", "::ffff:127.0.0.1", "::ffff:10.1.2.3", "::ffff:169.254.169.254", "::127.0.0.1",
		"fc00::1", "fd12:3456::1", "fe80::1", "fe80::1%eth0", "fec0::1", "ff02::1", "ff05::2",
		"64:ff9b::a00:1", "64:ff9b::7f00:1", "64:ff9b:1::1", "100::1", "2001::1", "2001:db8::1",
		"2001:2::1", "2001:20::1", "3fff::1", "5f00::1", "2002:a00:1::1", "2002:7f00:1::1",
	}
	for _, s := range blocked {
		if r := Reason(netip.MustParseAddr(s)); r == "" {
			t.Errorf("Reason(%s) = \"\", want it blocked", s)
		}
	}
	public := []string{
		"1.1.1.1", "8.8.8.8", "93.184.216.34", "202.45.144.10", "100.63.255.255", "100.128.0.0",
		"172.15.255.255", "172.32.0.0", "192.169.0.1", "223.255.255.255",
		"2606:4700:4700::1111", "2001:4860:4860::8888", "64:ff9b::808:808", "2002:808:808::1", "::ffff:8.8.8.8",
	}
	for _, s := range public {
		if r := Reason(netip.MustParseAddr(s)); r != "" {
			t.Errorf("Reason(%s) = %q, want it public", s, r)
		}
	}
}

func TestControl(t *testing.T) {
	var deny Policy
	if err := deny.Control("tcp4", "127.0.0.1:80", nil); !IsBlocked(err) {
		t.Errorf("Control(127.0.0.1:80) = %v, want a BlockedError", err)
	}
	if err := deny.Control("tcp6", "[fe80::1%eth0]:443", nil); !IsBlocked(err) {
		t.Errorf("Control([fe80::1%%eth0]:443) = %v, want a BlockedError", err)
	}
	if err := deny.Control("tcp4", "93.184.216.34:443", nil); err != nil {
		t.Errorf("Control(public) = %v, want nil", err)
	}
	if err := deny.Control("tcp", "not-an-address", nil); err == nil || IsBlocked(err) {
		t.Errorf("Control(garbage) = %v, want a plain error", err)
	}
	allow := Policy{AllowPrivate: true}
	if err := allow.Control("tcp4", "127.0.0.1:80", nil); err != nil {
		t.Errorf("AllowPrivate Control(127.0.0.1) = %v, want nil", err)
	}
}

type fakeResolver map[string][]netip.Addr

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	if a, ok := f[host]; ok {
		return a, nil
	}
	return nil, errors.New("no such host")
}

func TestLookupAllowedAndCheckHost(t *testing.T) {
	r := fakeResolver{
		"public.example": {netip.MustParseAddr("93.184.216.34")},
		"mixed.example":  {netip.MustParseAddr("93.184.216.34"), netip.MustParseAddr("10.0.0.5")},
		"rebind.example": {netip.MustParseAddr("169.254.169.254")},
	}
	var p Policy
	ctx := context.Background()

	if got, err := p.LookupAllowed(ctx, r, "mixed.example"); err != nil || len(got) != 1 || got[0].String() != "93.184.216.34" {
		t.Errorf("LookupAllowed(mixed) = %v, %v; want only the public address", got, err)
	}
	if _, err := p.LookupAllowed(ctx, r, "rebind.example"); !IsBlocked(err) {
		t.Errorf("LookupAllowed(metadata) = %v, want a BlockedError", err)
	}
	if _, err := p.LookupAllowed(ctx, r, "10.1.1.1"); !IsBlocked(err) {
		t.Errorf("LookupAllowed(private literal) = %v, want a BlockedError", err)
	}
	if err := p.CheckHost(ctx, r, "public.example"); err != nil {
		t.Errorf("CheckHost(public) = %v", err)
	}
	// A client that dials on its own may pick any address: one bad one fails it.
	if err := p.CheckHost(ctx, r, "mixed.example"); !IsBlocked(err) {
		t.Errorf("CheckHost(mixed) = %v, want a BlockedError", err)
	}
	if err := p.CheckHost(ctx, r, "missing.example"); err == nil || IsBlocked(err) {
		t.Errorf("CheckHost(unresolvable) = %v, want the lookup error", err)
	}
	if err := (Policy{AllowPrivate: true}).CheckHost(ctx, r, "rebind.example"); err != nil {
		t.Errorf("AllowPrivate CheckHost = %v, want nil", err)
	}
}
