package outboundgroup

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	"github.com/metacubex/mihomo/common/callback"
	N "github.com/metacubex/mihomo/common/net"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/log"
)

const (
	fallbackDialTimeout     = 3 * time.Second
	fallbackFailWindow      = 30 * time.Second
	fallbackFailThreshold   = 2
	fallbackBaseCooldown    = 30 * time.Second
	fallbackMaxCooldown     = 5 * time.Minute
	fallbackMaxDialAttempts = 3
	fallbackStallThreshold  = 4 * time.Second
)

type proxyFailState struct {
	failCount     int
	lastFail      time.Time
	cooldownUntil time.Time
	backoffLevel  int
}

type fallbackObserverConn struct {
	C.Conn
	closed    atomic.Bool
	done      atomic.Bool
	wroteAt   atomic.Int64
	onSuccess func()
	onFailure func(error)
}

func (c *fallbackObserverConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if !c.done.Load() {
		if n > 0 {
			if c.done.CompareAndSwap(false, true) && c.onSuccess != nil {
				c.onSuccess()
			}
		} else if err != nil && !c.closed.Load() {
			if c.done.CompareAndSwap(false, true) && c.onFailure != nil {
				c.onFailure(err)
			}
		}
	}
	return n, err
}

func (c *fallbackObserverConn) ReadBuffer(buffer *buf.Buffer) error {
	before := buffer.Len()
	err := c.Conn.ReadBuffer(buffer)
	if !c.done.Load() {
		if buffer.Len() > before {
			if c.done.CompareAndSwap(false, true) && c.onSuccess != nil {
				c.onSuccess()
			}
		} else if err != nil && !c.closed.Load() {
			if c.done.CompareAndSwap(false, true) && c.onFailure != nil {
				c.onFailure(err)
			}
		}
	}
	return err
}

func (c *fallbackObserverConn) Write(b []byte) (int, error) {
	n, err := c.Conn.Write(b)
	if !c.done.Load() {
		if err != nil {
			if !c.closed.Load() && c.done.CompareAndSwap(false, true) && c.onFailure != nil {
				c.onFailure(err)
			}
		} else if n > 0 && c.wroteAt.Load() == 0 {
			c.wroteAt.CompareAndSwap(0, time.Now().UnixNano())
		}
	}
	return n, err
}

func (c *fallbackObserverConn) WriteBuffer(buffer *buf.Buffer) error {
	dataLen := buffer.Len()
	err := c.Conn.WriteBuffer(buffer)
	if !c.done.Load() {
		if err != nil {
			if !c.closed.Load() && c.done.CompareAndSwap(false, true) && c.onFailure != nil {
				c.onFailure(err)
			}
		} else if dataLen > 0 && c.wroteAt.Load() == 0 {
			c.wroteAt.CompareAndSwap(0, time.Now().UnixNano())
		}
	}
	return err
}

func (c *fallbackObserverConn) Close() error {
	c.closed.Store(true)
	if !c.done.Load() {
		if w := c.wroteAt.Load(); w > 0 && time.Since(time.Unix(0, w)) >= fallbackStallThreshold {
			if c.done.CompareAndSwap(false, true) && c.onFailure != nil {
				c.onFailure(context.DeadlineExceeded)
			}
		}
	}
	return c.Conn.Close()
}

func (c *fallbackObserverConn) ReaderReplaceable() bool {
	return c.done.Load()
}

func (c *fallbackObserverConn) WriterReplaceable() bool {
	return c.done.Load()
}

func (c *fallbackObserverConn) Upstream() any {
	return c.Conn
}

var _ N.ExtendedConn = (*fallbackObserverConn)(nil)

type FallbackOption struct{}

type Fallback struct {
	*GroupBase
	disableUDP     bool
	testUrl        string
	selected       string
	expectedStatus string

	stateMux    sync.Mutex
	proxyStates map[string]*proxyFailState
}

func isSkippableAdapter(adapterType C.AdapterType) bool {
	return adapterType == C.Direct ||
		adapterType == C.Compatible ||
		adapterType == C.Reject ||
		adapterType == C.Pass ||
		adapterType == C.RejectDrop
}

func (f *Fallback) isDemotedLocked(name string, now time.Time) bool {
	st, ok := f.proxyStates[name]
	if !ok {
		return false
	}
	return now.Before(st.cooldownUntil)
}

func (f *Fallback) recordProxyFailure(proxy C.Proxy, immediate bool) {
	if proxy == nil || isSkippableAdapter(proxy.Type()) {
		return
	}

	now := time.Now()
	name := proxy.Name()
	triggerHealthCheck := false

	f.stateMux.Lock()
	if f.proxyStates == nil {
		f.proxyStates = make(map[string]*proxyFailState)
	}
	st, ok := f.proxyStates[name]
	if !ok {
		st = &proxyFailState{}
		f.proxyStates[name] = st
	}

	if !st.lastFail.IsZero() && now.Sub(st.lastFail) > fallbackFailWindow {
		st.failCount = 0
	}
	st.failCount++
	st.lastFail = now

	if immediate || st.failCount >= fallbackFailThreshold {
		cooldown := fallbackBaseCooldown << uint(st.backoffLevel)
		if cooldown > fallbackMaxCooldown {
			cooldown = fallbackMaxCooldown
		}
		if st.backoffLevel < 4 {
			st.backoffLevel++
		}
		st.cooldownUntil = now.Add(cooldown)
		st.failCount = 0
		if f.selected == name {
			f.selected = ""
		}
		triggerHealthCheck = true
		log.Warnln("Fallback[%s]: proxy %s demoted for %v after failure", f.Name(), name, cooldown)
	}
	f.stateMux.Unlock()

	if triggerHealthCheck {
		go f.healthCheck()
	}
}

func (f *Fallback) recordProxySuccess(proxy C.Proxy) {
	if proxy == nil {
		return
	}

	name := proxy.Name()
	f.stateMux.Lock()
	if st, ok := f.proxyStates[name]; ok {
		st.failCount = 0
		st.backoffLevel = 0
		st.cooldownUntil = time.Time{}
	}
	f.stateMux.Unlock()

	f.onDialSuccess()
}

func (f *Fallback) dialTimeoutForAttempt() time.Duration {
	if f.testTimeout > 0 {
		t := time.Duration(f.testTimeout) * time.Millisecond
		if t < fallbackDialTimeout {
			return t
		}
	}
	return fallbackDialTimeout
}

func (f *Fallback) Now() string {
	proxy := f.findAliveProxy(false)
	return proxy.Name()
}

// DialContext implements C.ProxyAdapter
func (f *Fallback) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	candidates := f.getCandidates(true)
	maxAttempts := len(candidates)
	if maxAttempts > fallbackMaxDialAttempts {
		maxAttempts = fallbackMaxDialAttempts
	}

	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		proxy := candidates[i]
		attemptCtx := ctx
		var cancel context.CancelFunc
		if i < maxAttempts-1 {
			attemptCtx, cancel = context.WithTimeout(ctx, f.dialTimeoutForAttempt())
		}

		c, err := proxy.DialContext(attemptCtx, metadata)
		if cancel != nil {
			cancel()
		}

		if err == nil && c != nil {
			c.AppendToChains(f)
			if N.NeedHandshake(c) {
				p := proxy
				c = callback.NewFirstWriteCallBackConn(c, func(wErr error) {
					if wErr == nil {
						f.onDialSuccess()
					} else if !errors.Is(wErr, context.Canceled) && !errors.Is(wErr, C.ErrNotSupport) {
						f.recordProxyFailure(p, true)
						f.onDialFailed(p.Type(), wErr, f.healthCheck)
					}
				})
			}
			if !isSkippableAdapter(proxy.Type()) {
				p := proxy
				c = &fallbackObserverConn{
					Conn: c,
					onSuccess: func() {
						f.recordProxySuccess(p)
					},
					onFailure: func(ioErr error) {
						f.recordProxyFailure(p, false)
						f.onDialFailed(p.Type(), ioErr, f.healthCheck)
					},
				}
			}
			return c, nil
		}

		lastErr = err
		if ctx.Err() != nil || errors.Is(err, C.ErrNotSupport) {
			return nil, err
		}

		f.recordProxyFailure(proxy, true)
		f.onDialFailed(proxy.Type(), err, f.healthCheck)
	}

	return nil, lastErr
}

// ListenPacketContext implements C.ProxyAdapter
func (f *Fallback) ListenPacketContext(ctx context.Context, metadata *C.Metadata) (C.PacketConn, error) {
	candidates := f.getCandidates(true)
	maxAttempts := len(candidates)
	if maxAttempts > fallbackMaxDialAttempts {
		maxAttempts = fallbackMaxDialAttempts
	}

	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		proxy := candidates[i]
		pc, err := proxy.ListenPacketContext(ctx, metadata)
		if err == nil {
			pc.AppendToChains(f)
			return pc, nil
		}

		lastErr = err
		if ctx.Err() != nil || errors.Is(err, C.ErrNotSupport) {
			return nil, err
		}

		f.recordProxyFailure(proxy, false)
	}

	return nil, lastErr
}

// SupportUDP implements C.ProxyAdapter
func (f *Fallback) SupportUDP() bool {
	if f.disableUDP {
		return false
	}

	proxy := f.findAliveProxy(false)
	return proxy.SupportUDP()
}

// IsL3Protocol implements C.ProxyAdapter
func (f *Fallback) IsL3Protocol(metadata *C.Metadata) bool {
	return f.findAliveProxy(false).IsL3Protocol(metadata)
}

// MarshalJSON implements C.ProxyAdapter
func (f *Fallback) MarshalJSON() ([]byte, error) {
	all := []string{}
	for _, proxy := range f.GetProxies(false) {
		all = append(all, proxy.Name())
	}
	f.stateMux.Lock()
	selected := f.selected
	f.stateMux.Unlock()
	return json.Marshal(map[string]any{
		"type":           f.Type().String(),
		"now":            f.Now(),
		"all":            all,
		"testUrl":        f.testUrl,
		"expectedStatus": f.expectedStatus,
		"fixed":          selected,
		"hidden":         f.Hidden(),
		"icon":           f.Icon(),
		"emptyFallback":  f.EmptyFallback().Name(),
	})
}

// Unwrap implements C.ProxyAdapter
func (f *Fallback) Unwrap(metadata *C.Metadata, touch bool) C.Proxy {
	proxy := f.findAliveProxy(touch)
	return proxy
}

func (f *Fallback) getCandidates(touch bool) []C.Proxy {
	proxies := f.GetProxies(touch)
	now := time.Now()

	f.stateMux.Lock()
	selected := f.selected
	var selectedProxy C.Proxy
	if len(selected) > 0 {
		for _, proxy := range proxies {
			if proxy.Name() == selected {
				if proxy.AliveForTestUrl(f.testUrl) && !f.isDemotedLocked(selected, now) {
					selectedProxy = proxy
				} else {
					f.selected = ""
				}
				break
			}
		}
	}

	aliveHealthy := make([]C.Proxy, 0, len(proxies))
	aliveDemoted := make([]C.Proxy, 0)
	otherHealthy := make([]C.Proxy, 0)
	otherDemoted := make([]C.Proxy, 0)

	if selectedProxy != nil {
		aliveHealthy = append(aliveHealthy, selectedProxy)
	}

	for _, proxy := range proxies {
		if selectedProxy != nil && proxy.Name() == selectedProxy.Name() {
			continue
		}
		demoted := f.isDemotedLocked(proxy.Name(), now)
		if proxy.AliveForTestUrl(f.testUrl) {
			if !demoted {
				aliveHealthy = append(aliveHealthy, proxy)
			} else {
				aliveDemoted = append(aliveDemoted, proxy)
			}
		} else {
			if !demoted {
				otherHealthy = append(otherHealthy, proxy)
			} else {
				otherDemoted = append(otherDemoted, proxy)
			}
		}
	}
	f.stateMux.Unlock()

	candidates := make([]C.Proxy, 0, len(proxies))
	candidates = append(candidates, aliveHealthy...)
	candidates = append(candidates, aliveDemoted...)
	candidates = append(candidates, otherHealthy...)
	candidates = append(candidates, otherDemoted...)
	return candidates
}

func (f *Fallback) findAliveProxy(touch bool) C.Proxy {
	candidates := f.getCandidates(touch)
	return candidates[0]
}

func (f *Fallback) Set(name string) error {
	var p C.Proxy
	for _, proxy := range f.GetProxies(false) {
		if proxy.Name() == name {
			p = proxy
			break
		}
	}

	if p == nil {
		return errors.New("proxy not exist")
	}

	f.stateMux.Lock()
	f.selected = name
	delete(f.proxyStates, name)
	f.stateMux.Unlock()

	if !p.AliveForTestUrl(f.testUrl) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(5000))
		defer cancel()
		expectedStatus, _ := utils.NewUnsignedRanges[uint16](f.expectedStatus)
		_, _ = p.URLTest(ctx, f.testUrl, expectedStatus)
	}

	return nil
}

func (f *Fallback) ForceSet(name string) {
	f.stateMux.Lock()
	f.selected = name
	delete(f.proxyStates, name)
	f.stateMux.Unlock()
}

func (f *Fallback) Providers() []P.ProxyProvider {
	return f.providers
}

func (f *Fallback) Proxies() []C.Proxy {
	return f.GetProxies(false)
}

func NewFallback(option GroupCommonOption, fallbackOption FallbackOption, emptyFallback C.Proxy, providers []P.ProxyProvider) (*Fallback, error) {
	return &Fallback{
		GroupBase: NewGroupBase(GroupBaseOption{
			Name:           option.Name,
			Type:           C.Fallback,
			Hidden:         option.Hidden,
			Icon:           option.Icon,
			Filter:         option.Filter,
			ExcludeFilter:  option.ExcludeFilter,
			ExcludeType:    option.ExcludeType,
			TestTimeout:    option.TestTimeout,
			MaxFailedTimes: option.MaxFailedTimes,
			EmptyFallback:  emptyFallback,
			Providers:      providers,
		}),
		disableUDP:     option.DisableUDP,
		testUrl:        option.URL,
		expectedStatus: option.ExpectedStatus,
		proxyStates:    make(map[string]*proxyFailState),
	}, nil
}
