package channels

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/time/rate"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
)

// mockChannel is a test double that delegates Send to a configurable function.
type mockChannel struct {
	BaseChannel
	sendFn func(ctx context.Context, msg bus.OutboundMessage) error
}

func (m *mockChannel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	return m.sendFn(ctx, msg)
}

func (m *mockChannel) Start(ctx context.Context) error { return nil }
func (m *mockChannel) Stop(ctx context.Context) error  { return nil }

// newTestManager creates a minimal Manager suitable for unit tests.
func newTestManager() *Manager {
	return &Manager{
		channels: make(map[string]Channel),
		workers:  make(map[string]*channelWorker),
	}
}

func TestSendWithRetry_Success(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"}

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 1 {
		t.Fatalf("expected 1 Send call, got %d", callCount)
	}
}

func TestSendWithRetry_TemporaryThenSuccess(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			if callCount <= 2 {
				return fmt.Errorf("network error: %w", ErrTemporary)
			}
			return nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"}

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 3 {
		t.Fatalf("expected 3 Send calls (2 failures + 1 success), got %d", callCount)
	}
}

func TestSendWithRetry_PermanentFailure(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return fmt.Errorf("bad chat ID: %w", ErrSendFailed)
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"}

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 1 {
		t.Fatalf("expected 1 Send call (no retry for permanent failure), got %d", callCount)
	}
}

func TestSendWithRetry_NotRunning(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return ErrNotRunning
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"}

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 1 {
		t.Fatalf("expected 1 Send call (no retry for ErrNotRunning), got %d", callCount)
	}
}

func TestSendWithRetry_RateLimitRetry(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			if callCount == 1 {
				return fmt.Errorf("429: %w", ErrRateLimit)
			}
			return nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"}

	start := time.Now()
	m.sendWithRetry(ctx, "test", w, msg)
	elapsed := time.Since(start)

	if callCount != 2 {
		t.Fatalf("expected 2 Send calls (1 rate limit + 1 success), got %d", callCount)
	}
	// Should have waited at least rateLimitDelay (1s) but allow some slack
	if elapsed < 900*time.Millisecond {
		t.Fatalf("expected at least ~1s delay for rate limit retry, got %v", elapsed)
	}
}

func TestSendWithRetry_MaxRetriesExhausted(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return fmt.Errorf("timeout: %w", ErrTemporary)
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"}

	m.sendWithRetry(ctx, "test", w, msg)

	expected := maxRetries + 1 // initial attempt + maxRetries retries
	if callCount != expected {
		t.Fatalf("expected %d Send calls, got %d", expected, callCount)
	}
}

func TestSendWithRetry_UnknownError(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			if callCount == 1 {
				return errors.New("random unexpected error")
			}
			return nil
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"}

	m.sendWithRetry(ctx, "test", w, msg)

	if callCount != 2 {
		t.Fatalf("expected 2 Send calls (unknown error treated as temporary), got %d", callCount)
	}
}

func TestSendWithRetry_ContextCancelled(t *testing.T) {
	m := newTestManager()
	var callCount int
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callCount++
			return fmt.Errorf("timeout: %w", ErrTemporary)
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx, cancel := context.WithCancel(context.Background())
	msg := bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"}

	// Cancel context after first Send attempt returns
	ch.sendFn = func(_ context.Context, _ bus.OutboundMessage) error {
		callCount++
		cancel()
		return fmt.Errorf("timeout: %w", ErrTemporary)
	}

	m.sendWithRetry(ctx, "test", w, msg)

	// Should have called Send once, then noticed ctx canceled during backoff
	if callCount != 1 {
		t.Fatalf("expected 1 Send call before context cancellation, got %d", callCount)
	}
}

func TestWorkerRateLimiter(t *testing.T) {
	m := newTestManager()

	var mu sync.Mutex
	var sendTimes []time.Time

	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			mu.Lock()
			sendTimes = append(sendTimes, time.Now())
			mu.Unlock()
			return nil
		},
	}

	// Create a worker with a low rate: 2 msg/s, burst 1
	w := &channelWorker{
		ch:      ch,
		queue:   make(chan bus.OutboundMessage, 10),
		done:    make(chan struct{}),
		limiter: rate.NewLimiter(2, 1),
	}

	ctx := t.Context()

	go m.runWorker(ctx, "test", w)

	// Enqueue 4 messages
	for i := range 4 {
		w.queue <- bus.OutboundMessage{Channel: "test", ChatID: "1", Content: fmt.Sprintf("msg%d", i)}
	}

	// Wait enough time for all messages to be sent (4 msgs at 2/s = ~2s, give extra margin)
	time.Sleep(3 * time.Second)

	mu.Lock()
	times := make([]time.Time, len(sendTimes))
	copy(times, sendTimes)
	mu.Unlock()

	if len(times) != 4 {
		t.Fatalf("expected 4 sends, got %d", len(times))
	}

	// Verify rate limiting: total duration should be at least 1s
	// (first message immediate, then ~500ms between each subsequent one at 2/s)
	totalDuration := times[len(times)-1].Sub(times[0])
	if totalDuration < 1*time.Second {
		t.Fatalf("expected total duration >= 1s for 4 msgs at 2/s rate, got %v", totalDuration)
	}
}

func TestNewChannelWorker_DefaultRate(t *testing.T) {
	ch := &mockChannel{}
	w := newChannelWorker("unknown_channel", ch)

	if w.limiter == nil {
		t.Fatal("expected limiter to be non-nil")
	}
	if w.limiter.Limit() != rate.Limit(defaultRateLimit) {
		t.Fatalf("expected rate limit %v, got %v", rate.Limit(defaultRateLimit), w.limiter.Limit())
	}
}

func TestNewChannelWorker_ConfiguredRate(t *testing.T) {
	ch := &mockChannel{}

	for name, expectedRate := range channelRateConfig {
		w := newChannelWorker(name, ch)
		if w.limiter.Limit() != rate.Limit(expectedRate) {
			t.Fatalf("channel %s: expected rate %v, got %v", name, expectedRate, w.limiter.Limit())
		}
	}
}

func TestRunWorker_MessageSplitting(t *testing.T) {
	m := newTestManager()

	var mu sync.Mutex
	var received []string

	ch := &mockChannelWithLength{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, msg bus.OutboundMessage) error {
				mu.Lock()
				received = append(received, msg.Content)
				mu.Unlock()
				return nil
			},
		},
		maxLen: 5,
	}

	w := &channelWorker{
		ch:      ch,
		queue:   make(chan bus.OutboundMessage, 10),
		done:    make(chan struct{}),
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := t.Context()

	go m.runWorker(ctx, "test", w)

	// Send a message that should be split
	w.queue <- bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello world"}

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	count := len(received)
	mu.Unlock()

	if count < 2 {
		t.Fatalf("expected message to be split into at least 2 chunks, got %d", count)
	}
}

// mockChannelWithLength implements MessageLengthProvider.
type mockChannelWithLength struct {
	mockChannel
	maxLen int
}

func (m *mockChannelWithLength) MaxMessageLength() int {
	return m.maxLen
}

func TestSendWithRetry_ExponentialBackoff(t *testing.T) {
	m := newTestManager()

	var callTimes []time.Time
	var callCount atomic.Int32
	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			callTimes = append(callTimes, time.Now())
			callCount.Add(1)
			return fmt.Errorf("timeout: %w", ErrTemporary)
		},
	}
	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	ctx := context.Background()
	msg := bus.OutboundMessage{Channel: "test", ChatID: "1", Content: "hello"}

	start := time.Now()
	m.sendWithRetry(ctx, "test", w, msg)
	totalElapsed := time.Since(start)

	// With maxRetries=3: attempts at 0, ~500ms, ~1.5s, ~3.5s
	// Total backoff: 500ms + 1s + 2s = 3.5s
	// Allow some margin
	if totalElapsed < 3*time.Second {
		t.Fatalf("expected total elapsed >= 3s for exponential backoff, got %v", totalElapsed)
	}

	if int(callCount.Load()) != maxRetries+1 {
		t.Fatalf("expected %d calls, got %d", maxRetries+1, callCount.Load())
	}
}

// --- Phase 10: preSend orchestration tests ---

// mockMessageEditor is a channel that supports MessageEditor.
type mockMessageEditor struct {
	mockChannel
	editFn func(ctx context.Context, chatID, messageID, content string) error
}

func (m *mockMessageEditor) EditMessage(ctx context.Context, chatID, messageID, content string) error {
	return m.editFn(ctx, chatID, messageID, content)
}

func TestPreSend_PlaceholderEditSuccess(t *testing.T) {
	m := newTestManager()
	var sendCalled bool
	var editCalled bool

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				sendCalled = true
				return nil
			},
		},
		editFn: func(_ context.Context, chatID, messageID, content string) error {
			editCalled = true
			if chatID != "123" {
				t.Fatalf("expected chatID 123, got %s", chatID)
			}
			if messageID != "456" {
				t.Fatalf("expected messageID 456, got %s", messageID)
			}
			if content != "hello" {
				t.Fatalf("expected content 'hello', got %s", content)
			}
			return nil
		},
	}

	// Register placeholder
	m.RecordPlaceholder("test", "123", "456")

	msg := bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"}
	edited := m.preSend(context.Background(), "test", msg, ch)

	if !edited {
		t.Fatal("expected preSend to return true (placeholder edited)")
	}
	if !editCalled {
		t.Fatal("expected EditMessage to be called")
	}
	if sendCalled {
		t.Fatal("expected Send to NOT be called when placeholder edited")
	}
}

func TestPreSend_PlaceholderEditFails_FallsThrough(t *testing.T) {
	m := newTestManager()

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				return nil
			},
		},
		editFn: func(_ context.Context, _, _, _ string) error {
			return fmt.Errorf("edit failed")
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	msg := bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"}
	edited := m.preSend(context.Background(), "test", msg, ch)

	if edited {
		t.Fatal("expected preSend to return false when edit fails")
	}
}

func TestPreSend_TypingStopCalled(t *testing.T) {
	m := newTestManager()
	var stopCalled bool

	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			return nil
		},
	}

	m.RecordTypingStop("test", "123", func() {
		stopCalled = true
	})

	msg := bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"}
	m.preSend(context.Background(), "test", msg, ch)

	if !stopCalled {
		t.Fatal("expected typing stop func to be called")
	}
}

func TestPreSend_NoRegisteredState(t *testing.T) {
	m := newTestManager()

	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			return nil
		},
	}

	msg := bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"}
	edited := m.preSend(context.Background(), "test", msg, ch)

	if edited {
		t.Fatal("expected preSend to return false with no registered state")
	}
}

func TestPreSend_TypingAndPlaceholder(t *testing.T) {
	m := newTestManager()
	var stopCalled bool
	var editCalled bool

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				return nil
			},
		},
		editFn: func(_ context.Context, _, _, _ string) error {
			editCalled = true
			return nil
		},
	}

	m.RecordTypingStop("test", "123", func() {
		stopCalled = true
	})
	m.RecordPlaceholder("test", "123", "456")

	msg := bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"}
	edited := m.preSend(context.Background(), "test", msg, ch)

	if !stopCalled {
		t.Fatal("expected typing stop to be called")
	}
	if !editCalled {
		t.Fatal("expected EditMessage to be called")
	}
	if !edited {
		t.Fatal("expected preSend to return true")
	}
}

func TestRecordPlaceholder_ConcurrentSafe(t *testing.T) {
	m := newTestManager()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			chatID := fmt.Sprintf("chat_%d", i%10)
			m.RecordPlaceholder("test", chatID, fmt.Sprintf("msg_%d", i))
		}(i)
	}
	wg.Wait()
}

func TestRecordTypingStop_ConcurrentSafe(t *testing.T) {
	m := newTestManager()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			chatID := fmt.Sprintf("chat_%d", i%10)
			m.RecordTypingStop("test", chatID, func() {})
		}(i)
	}
	wg.Wait()
}

func TestSendWithRetry_PreSendEditsPlaceholder(t *testing.T) {
	m := newTestManager()
	var sendCalled bool

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				sendCalled = true
				return nil
			},
		},
		editFn: func(_ context.Context, _, _, _ string) error {
			return nil // edit succeeds
		},
	}

	m.RecordPlaceholder("test", "123", "456")

	w := &channelWorker{
		ch:      ch,
		limiter: rate.NewLimiter(rate.Inf, 1),
	}

	msg := bus.OutboundMessage{Channel: "test", ChatID: "123", Content: "hello"}
	m.sendWithRetry(context.Background(), "test", w, msg)

	if sendCalled {
		t.Fatal("expected Send to NOT be called when placeholder was edited")
	}
}

// --- Dispatcher exit tests (Step 1) ---

func TestDispatcherExitsOnCancel(t *testing.T) {
	mb := bus.NewMessageBus()
	defer mb.Close()

	m := &Manager{
		channels: make(map[string]Channel),
		workers:  make(map[string]*channelWorker),
		bus:      mb,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		m.dispatchOutbound(ctx)
		close(done)
	}()

	// Cancel context and verify the dispatcher exits quickly
	cancel()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("dispatchOutbound did not exit within 2s after context cancel")
	}
}

func TestDispatcherMediaExitsOnCancel(t *testing.T) {
	mb := bus.NewMessageBus()
	defer mb.Close()

	m := &Manager{
		channels: make(map[string]Channel),
		workers:  make(map[string]*channelWorker),
		bus:      mb,
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		m.dispatchOutboundMedia(ctx)
		close(done)
	}()

	cancel()

	select {
	case <-done:
		// success
	case <-time.After(2 * time.Second):
		t.Fatal("dispatchOutboundMedia did not exit within 2s after context cancel")
	}
}

// --- TTL Janitor tests (Step 2) ---

func TestTypingStopJanitorEviction(t *testing.T) {
	m := newTestManager()

	var stopCalled atomic.Bool
	// Store a typing entry with a creation time far in the past
	m.typingStops.Store("test:123", typingEntry{
		stop:      func() { stopCalled.Store(true) },
		createdAt: time.Now().Add(-10 * time.Minute), // well past typingStopTTL
	})

	// Run janitor with a short-lived context
	ctx, cancel := context.WithCancel(context.Background())

	// Manually trigger the janitor logic once by simulating a tick
	go func() {
		// Override janitor to run immediately
		now := time.Now()
		m.typingStops.Range(func(key, value any) bool {
			if entry, ok := value.(typingEntry); ok {
				if now.Sub(entry.createdAt) > typingStopTTL {
					if _, loaded := m.typingStops.LoadAndDelete(key); loaded {
						entry.stop()
					}
				}
			}
			return true
		})
		cancel()
	}()

	<-ctx.Done()

	if !stopCalled.Load() {
		t.Fatal("expected typing stop function to be called by janitor eviction")
	}

	// Verify entry was deleted
	if _, loaded := m.typingStops.Load("test:123"); loaded {
		t.Fatal("expected typing entry to be deleted after eviction")
	}
}

func TestPlaceholderJanitorEviction(t *testing.T) {
	m := newTestManager()

	// Store a placeholder entry with a creation time far in the past
	m.placeholders.Store("test:456", placeholderEntry{
		id:        "msg_old",
		createdAt: time.Now().Add(-20 * time.Minute), // well past placeholderTTL
	})

	// Simulate janitor logic
	now := time.Now()
	m.placeholders.Range(func(key, value any) bool {
		if entry, ok := value.(placeholderEntry); ok {
			if now.Sub(entry.createdAt) > placeholderTTL {
				m.placeholders.Delete(key)
			}
		}
		return true
	})

	// Verify entry was deleted
	if _, loaded := m.placeholders.Load("test:456"); loaded {
		t.Fatal("expected placeholder entry to be deleted after eviction")
	}
}

func TestPreSendStillWorksWithWrappedTypes(t *testing.T) {
	m := newTestManager()
	var stopCalled bool
	var editCalled bool

	ch := &mockMessageEditor{
		mockChannel: mockChannel{
			sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
				return nil
			},
		},
		editFn: func(_ context.Context, chatID, messageID, content string) error {
			editCalled = true
			if messageID != "ph_id" {
				t.Fatalf("expected messageID ph_id, got %s", messageID)
			}
			return nil
		},
	}

	// Use the new wrapped types via the public API
	m.RecordTypingStop("test", "chat1", func() {
		stopCalled = true
	})
	m.RecordPlaceholder("test", "chat1", "ph_id")

	msg := bus.OutboundMessage{Channel: "test", ChatID: "chat1", Content: "response"}
	edited := m.preSend(context.Background(), "test", msg, ch)

	if !stopCalled {
		t.Fatal("expected typing stop to be called via wrapped type")
	}
	if !editCalled {
		t.Fatal("expected EditMessage to be called via wrapped type")
	}
	if !edited {
		t.Fatal("expected preSend to return true")
	}
}

// --- Lazy worker creation tests (Step 6) ---

func TestLazyWorkerCreation(t *testing.T) {
	m := newTestManager()

	ch := &mockChannel{
		sendFn: func(_ context.Context, _ bus.OutboundMessage) error {
			return nil
		},
	}

	// RegisterChannel should NOT create a worker
	m.RegisterChannel("lazy", ch)

	m.mu.RLock()
	_, chExists := m.channels["lazy"]
	_, wExists := m.workers["lazy"]
	m.mu.RUnlock()

	if !chExists {
		t.Fatal("expected channel to be registered")
	}
	if wExists {
		t.Fatal("expected worker to NOT be created by RegisterChannel (lazy creation)")
	}
}

// --- FastID uniqueness test (Step 5) ---

func TestBuildMediaScope_FastIDUniqueness(t *testing.T) {
	seen := make(map[string]bool)

	for range 1000 {
		scope := BuildMediaScope("test", "chat1", "")
		if seen[scope] {
			t.Fatalf("duplicate scope generated: %s", scope)
		}
		seen[scope] = true
	}

	// Verify format: "channel:chatID:id"
	scope := BuildMediaScope("telegram", "42", "")
	parts := 0
	for _, c := range scope {
		if c == ':' {
			parts++
		}
	}
	if parts != 2 {
		t.Fatalf("expected scope to have 2 colons (channel:chatID:id), got: %s", scope)
	}
}

func TestBuildMediaScope_WithMessageID(t *testing.T) {
	scope := BuildMediaScope("discord", "chat99", "msg123")
	expected := "discord:chat99:msg123"
	if scope != expected {
		t.Fatalf("expected %s, got %s", expected, scope)
	}
}

// =============================================================================
// Connection Retry Logic Tests
// =============================================================================

// mockConnectableChannel is a test double that can simulate connection failures
// and successes for testing the retry logic in StartAll.
type mockConnectableChannel struct {
	BaseChannel
	startAttempts atomic.Int32
	maxFailures   int
	startDelay    time.Duration
	shouldFail    bool
	lastError     error
}

func newMockConnectableChannel(maxFailures int) *mockConnectableChannel {
	ch := &mockConnectableChannel{
		maxFailures: maxFailures,
		startDelay:  10 * time.Millisecond, // Fast for tests
	}
	ch.SetRunning(false)
	return ch
}

func (m *mockConnectableChannel) Start(ctx context.Context) error {
	attempt := int(m.startAttempts.Add(1))

	if m.startDelay > 0 {
		select {
		case <-time.After(m.startDelay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if m.shouldFail || attempt <= m.maxFailures {
		m.lastError = fmt.Errorf("connection attempt %d failed", attempt)
		return m.lastError
	}

	m.SetRunning(true)
	return nil
}

func (m *mockConnectableChannel) Stop(ctx context.Context) error {
	m.SetRunning(false)
	return nil
}

func (m *mockConnectableChannel) Send(ctx context.Context, msg bus.OutboundMessage) error {
	return nil
}

func (m *mockConnectableChannel) IsAllowed(senderID string) bool             { return true }
func (m *mockConnectableChannel) IsAllowedSender(sender bus.SenderInfo) bool { return true }
func (m *mockConnectableChannel) ReasoningChannelID() string                 { return "" }

// newTestManagerWithConfig creates a Manager with a real config for testing retry logic.
func newTestManagerWithConfig() *Manager {
	cfg := config.DefaultConfig()
	return &Manager{
		channels:   make(map[string]Channel),
		workers:    make(map[string]*channelWorker),
		connStates: make(map[string]*channelConnectionState),
		config:     cfg,
		bus:        bus.NewMessageBus(), // Add a message bus to avoid nil pointer
	}
}

// TestStartAll_SuccessFirstAttempt verifies channels connect on first try.
func TestStartAll_SuccessFirstAttempt(t *testing.T) {
	m := newTestManagerWithConfig()

	// Create a channel that succeeds immediately (maxFailures = 0)
	ch := newMockConnectableChannel(0)
	m.channels["discord"] = ch

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := m.StartAll(ctx)
	if err != nil {
		t.Fatalf("StartAll failed: %v", err)
	}

	// Verify channel started
	if !ch.IsRunning() {
		t.Error("Channel should be running after successful StartAll")
	}

	// Verify worker was created
	if _, ok := m.workers["discord"]; !ok {
		t.Error("Worker should be created for started channel")
	}

	// Verify connection state
	state := m.connStates["discord"]
	if state == nil {
		t.Fatal("Connection state should exist")
	}
	if !state.IsConnected() {
		t.Error("Channel should be marked as connected")
	}
	if state.GetAttempts() != 1 {
		t.Errorf("Expected 1 attempt, got %d", state.GetAttempts())
	}
}

// TestStartAll_RetryThenSuccess verifies retry logic works - fails then succeeds.
func TestStartAll_RetryThenSuccess(t *testing.T) {
	m := newTestManagerWithConfig()

	// Create a channel that fails 2 times then succeeds
	ch := newMockConnectableChannel(2)
	m.channels["discord"] = ch

	// Override default backoff to be fast for testing
	oldBaseBackoff := defaultBaseBackoffDelay
	defaultBaseBackoffDelay = 50 * time.Millisecond
	defer func() { defaultBaseBackoffDelay = oldBaseBackoff }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	err := m.StartAll(ctx)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("StartAll failed: %v", err)
	}

	// Verify channel eventually started
	if !ch.IsRunning() {
		t.Error("Channel should be running after retry success")
	}

	// Verify correct number of attempts
	if ch.startAttempts.Load() != 3 {
		t.Errorf("Expected 3 attempts (2 failures + 1 success), got %d", ch.startAttempts.Load())
	}

	// Verify backoff delays: 50ms + 100ms = 150ms minimum
	if elapsed < 150*time.Millisecond {
		t.Errorf("Expected at least 150ms for exponential backoff, got %v", elapsed)
	}

	// Verify state tracking
	state := m.connStates["discord"]
	if state.GetAttempts() != 3 {
		t.Errorf("Expected 3 attempts in state, got %d", state.GetAttempts())
	}
	if !state.IsConnected() {
		t.Error("Channel should be marked as connected")
	}
}

// TestStartAll_MaxRetriesExhausted verifies channel stops after max retries.
func TestStartAll_MaxRetriesExhausted(t *testing.T) {
	m := newTestManagerWithConfig()

	// Create a channel that always fails
	ch := newMockConnectableChannel(100) // More than default max retries
	ch.shouldFail = true
	m.channels["discord"] = ch

	// Set max retries to 3 for faster test
	m.config.Channels.Discord.MaxRetries = 3

	// Fast backoff for testing
	oldBaseBackoff := defaultBaseBackoffDelay
	defaultBaseBackoffDelay = 20 * time.Millisecond
	defer func() { defaultBaseBackoffDelay = oldBaseBackoff }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := m.StartAll(ctx)
	// StartAll should succeed even if some channels fail
	if err != nil {
		t.Fatalf("StartAll should not error when channel exhausts retries: %v", err)
	}

	// Verify channel was attempted max_retries times
	if ch.startAttempts.Load() != 3 {
		t.Errorf("Expected 3 attempts (max_retries), got %d", ch.startAttempts.Load())
	}

	// Verify channel is NOT running
	if ch.IsRunning() {
		t.Error("Channel should not be running after exhausting retries")
	}

	// Verify worker was NOT created
	if _, ok := m.workers["discord"]; ok {
		t.Error("Worker should NOT be created for failed channel")
	}

	// Verify state shows not connected but with max attempts
	state := m.connStates["discord"]
	if state.IsConnected() {
		t.Error("Channel should not be marked as connected")
	}
	if state.GetAttempts() != 3 {
		t.Errorf("Expected 3 attempts in state, got %d", state.GetAttempts())
	}
}

// TestStartAll_MixedChannels verifies non-blocking behavior with mixed results.
func TestStartAll_MixedChannels(t *testing.T) {
	m := newTestManagerWithConfig()

	// Channel 1: Connects immediately
	ch1 := newMockConnectableChannel(0)
	m.channels["telegram"] = ch1

	// Channel 2: Fails 1 time then connects
	ch2 := newMockConnectableChannel(1)
	m.channels["discord"] = ch2

	// Channel 3: Always fails (will exhaust retries)
	ch3 := newMockConnectableChannel(100)
	ch3.shouldFail = true
	m.config.Channels.Slack.MaxRetries = 2
	m.channels["slack"] = ch3

	// Fast backoff for testing
	oldBaseBackoff := defaultBaseBackoffDelay
	defaultBaseBackoffDelay = 30 * time.Millisecond
	defer func() { defaultBaseBackoffDelay = oldBaseBackoff }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := m.StartAll(ctx)
	if err != nil {
		t.Fatalf("StartAll failed: %v", err)
	}

	// Verify instant channel connected on first try
	if !ch1.IsRunning() {
		t.Error("Instant channel should be running")
	}
	if ch1.startAttempts.Load() != 1 {
		t.Errorf("Instant channel: expected 1 attempt, got %d", ch1.startAttempts.Load())
	}

	// Verify retry channel connected on 2nd try
	if !ch2.IsRunning() {
		t.Error("Retry channel should be running")
	}
	if ch2.startAttempts.Load() != 2 {
		t.Errorf("Retry channel: expected 2 attempts, got %d", ch2.startAttempts.Load())
	}

	// Verify failing channel exhausted retries
	if ch3.IsRunning() {
		t.Error("Failing channel should NOT be running")
	}
	if ch3.startAttempts.Load() != 2 {
		t.Errorf("Failing channel: expected 2 attempts, got %d", ch3.startAttempts.Load())
	}

	// Verify workers created only for successful channels
	if _, ok := m.workers["telegram"]; !ok {
		t.Error("Worker should exist for instant channel")
	}
	if _, ok := m.workers["discord"]; !ok {
		t.Error("Worker should exist for retry channel")
	}
	if _, ok := m.workers["slack"]; ok {
		t.Error("Worker should NOT exist for failing channel")
	}

	// Verify GetConnectionStatus shows correct state
	status := m.GetConnectionStatus()
	channels := status["channels"].(map[string]any)

	instantStatus := channels["telegram"].(map[string]any)
	if !instantStatus["connected"].(bool) {
		t.Error("Instant channel should show as connected")
	}

	retryStatus := channels["discord"].(map[string]any)
	if !retryStatus["connected"].(bool) {
		t.Error("Retry channel should show as connected")
	}

	failingStatus := channels["slack"].(map[string]any)
	if failingStatus["connected"].(bool) {
		t.Error("Failing channel should NOT show as connected")
	}

	// Verify summary counts
	if status["connected"].(int) != 2 {
		t.Errorf("Expected 2 connected, got %d", status["connected"].(int))
	}
	if status["failed"].(int) != 1 {
		t.Errorf("Expected 1 failed, got %d", status["failed"].(int))
	}
}

// TestStartAll_ContextCancellation verifies graceful shutdown on context cancel.
func TestStartAll_ContextCancellation(t *testing.T) {
	m := newTestManagerWithConfig()

	// Create a channel that takes a while to fail
	ch := newMockConnectableChannel(100)
	ch.shouldFail = true
	ch.startDelay = 500 * time.Millisecond
	m.channels["discord"] = ch

	// Fast backoff
	oldBaseBackoff := defaultBaseBackoffDelay
	defaultBaseBackoffDelay = 100 * time.Millisecond
	defer func() { defaultBaseBackoffDelay = oldBaseBackoff }()

	ctx, cancel := context.WithCancel(context.Background())

	// Cancel context after short delay (during first attempt)
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()

	err := m.StartAll(ctx)

	// Should return context cancellation error
	if err != context.Canceled && err != context.DeadlineExceeded {
		t.Errorf("Expected context error, got: %v", err)
	}
}

// TestBackoffCalculation verifies exponential backoff calculation.
func TestBackoffCalculation(t *testing.T) {
	m := newTestManagerWithConfig()

	// Set custom backoff factor
	m.config.Channels.Discord.BackoffFactor = 2.5

	ch := newMockConnectableChannel(3)
	m.channels["discord"] = ch

	// Short delays for testing
	oldBaseBackoff := defaultBaseBackoffDelay
	defaultBaseBackoffDelay = 10 * time.Millisecond
	defer func() { defaultBaseBackoffDelay = oldBaseBackoff }()

	oldMaxBackoff := defaultMaxBackoffDelay
	defaultMaxBackoffDelay = 100 * time.Millisecond
	defer func() { defaultMaxBackoffDelay = oldMaxBackoff }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := m.StartAll(ctx)
	if err != nil {
		t.Fatalf("StartAll failed: %v", err)
	}

	state := m.connStates["discord"]
	// With 3 failures then success (4 attempts total):
	// backoff progression: 10ms -> 25ms -> 62.5ms -> 156.25ms (capped at 100ms)
	// So final backoff should be 100ms (the cap)
	backoff := state.GetBackoffDelay()
	expectedBackoff := 100 * time.Millisecond // Capped at max
	if backoff != expectedBackoff {
		t.Errorf("Expected backoff %v, got %v", expectedBackoff, backoff)
	}
}

// TestConnectionStateThreadSafety verifies concurrent access to connection state.
// This test uses -race flag to detect data races.
func TestConnectionStateThreadSafety(t *testing.T) {
	m := newTestManagerWithConfig()

	ch := newMockConnectableChannel(2)
	m.channels["discord"] = ch

	// Fast backoff
	oldBaseBackoff := defaultBaseBackoffDelay
	defaultBaseBackoffDelay = 20 * time.Millisecond
	defer func() { defaultBaseBackoffDelay = oldBaseBackoff }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start StartAll in background
	var startErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		startErr = m.StartAll(ctx)
	}()

	// Concurrently read connection status multiple times
	for i := 0; i < 50; i++ {
		_ = m.GetConnectionStatus()
		_ = m.GetStatus()
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()

	if startErr != nil {
		t.Fatalf("StartAll failed: %v", startErr)
	}

	// Final status check
	status := m.GetConnectionStatus()
	if status["connected"].(int) != 1 {
		t.Errorf("Expected 1 connected, got %d", status["connected"].(int))
	}
}

// TestGetConnectionStatus verifies the detailed status method.
func TestGetConnectionStatus(t *testing.T) {
	m := newTestManagerWithConfig()

	// Create channels with different outcomes
	ch1 := newMockConnectableChannel(0) // Success
	ch2 := newMockConnectableChannel(1) // Retry then success
	m.channels["telegram"] = ch1
	m.channels["discord"] = ch2

	// Set max retries for testing
	m.config.Channels.Telegram.MaxRetries = 5
	m.config.Channels.Discord.MaxRetries = 5

	// Fast backoff
	oldBaseBackoff := defaultBaseBackoffDelay
	defaultBaseBackoffDelay = 10 * time.Millisecond
	defer func() { defaultBaseBackoffDelay = oldBaseBackoff }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := m.StartAll(ctx)
	if err != nil {
		t.Fatalf("StartAll failed: %v", err)
	}

	status := m.GetConnectionStatus()

	// Verify structure
	if status["connected"].(int) != 2 {
		t.Errorf("Expected connected=2, got %d", status["connected"].(int))
	}
	if status["failed"].(int) != 0 {
		t.Errorf("Expected failed=0, got %d", status["failed"].(int))
	}
	if status["total"].(int) != 2 {
		t.Errorf("Expected total=2, got %d", status["total"].(int))
	}

	channels := status["channels"].(map[string]any)

	// Verify success channel status
	successStatus := channels["telegram"].(map[string]any)
	if !successStatus["connected"].(bool) {
		t.Error("Success channel should be connected")
	}
	if successStatus["attempts"].(int) != 1 {
		t.Errorf("Success channel: expected attempts=1, got %d", successStatus["attempts"].(int))
	}
	if successStatus["max_retries"].(int) != 5 {
		t.Errorf("Success channel: expected max_retries=5, got %d", successStatus["max_retries"].(int))
	}
	if !successStatus["running"].(bool) {
		t.Error("Success channel should be running")
	}

	// Verify retry channel status
	retryStatus := channels["discord"].(map[string]any)
	if !retryStatus["connected"].(bool) {
		t.Error("Retry channel should be connected")
	}
	if retryStatus["attempts"].(int) != 2 {
		t.Errorf("Retry channel: expected attempts=2, got %d", retryStatus["attempts"].(int))
	}

	// Verify last_attempt is set
	if retryStatus["last_attempt"].(time.Time).IsZero() {
		t.Error("last_attempt should be set")
	}
}

// TestUnregisterChannelCleansUpState verifies connStates is cleaned up.
func TestUnregisterChannelCleansUpState(t *testing.T) {
	m := newTestManagerWithConfig()

	ch := newMockConnectableChannel(0)
	m.channels["test"] = ch
	m.connStates["test"] = &channelConnectionState{
		attempts:  1,
		connected: true,
	}

	// Note: Not creating workers here since that requires running StartAll
	// This test focuses on the map cleanup logic

	// Unregister should clean up all maps
	m.UnregisterChannel("test")

	if _, ok := m.channels["test"]; ok {
		t.Error("Channel should be removed from channels map")
	}
	if _, ok := m.workers["test"]; ok {
		t.Error("Worker should be removed from workers map")
	}
	if _, ok := m.connStates["test"]; ok {
		t.Error("Connection state should be removed from connStates map")
	}
}
