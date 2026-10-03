// PicoClaw - Ultra-lightweight personal AI agent
// Inspired by and based on nanobot: https://github.com/HKUDS/nanobot
// License: MIT
//
// Copyright (c) 2026 PicoClaw contributors

package channels

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/sipeed/picoclaw/pkg/bus"
	"github.com/sipeed/picoclaw/pkg/config"
	"github.com/sipeed/picoclaw/pkg/constants"
	"github.com/sipeed/picoclaw/pkg/health"
	"github.com/sipeed/picoclaw/pkg/logger"
	"github.com/sipeed/picoclaw/pkg/media"
)

const (
	defaultChannelQueueSize = 16
	defaultRateLimit        = 10 // default 10 msg/s
	maxRetries              = 3
	rateLimitDelay          = 1 * time.Second
	baseBackoff             = 500 * time.Millisecond
	maxBackoff              = 8 * time.Second

	janitorInterval = 10 * time.Second
	typingStopTTL   = 5 * time.Minute
	placeholderTTL  = 10 * time.Minute
)

// Connection retry defaults (variables for testability)
var (
	defaultBaseBackoffDelay = 5 * time.Second
	defaultMaxBackoffDelay  = 60 * time.Second
	defaultBackoffFactor    = 2.0
)

// typingEntry wraps a typing stop function with a creation timestamp for TTL eviction.
type typingEntry struct {
	stop      func()
	createdAt time.Time
}

// reactionEntry wraps a reaction undo function with a creation timestamp for TTL eviction.
type reactionEntry struct {
	undo      func()
	createdAt time.Time
}

// placeholderEntry wraps a placeholder ID with a creation timestamp for TTL eviction.
type placeholderEntry struct {
	id        string
	createdAt time.Time
}

// channelRateConfig maps channel name to per-second rate limit.
var channelRateConfig = map[string]float64{
	"telegram": 20,
	"discord":  1,
	"slack":    1,
	"line":     10,
}

type channelWorker struct {
	ch         Channel
	queue      chan bus.OutboundMessage
	mediaQueue chan bus.OutboundMediaMessage
	done       chan struct{}
	mediaDone  chan struct{}
	limiter    *rate.Limiter
}

// channelConnectionState tracks the connection retry state for a channel
type channelConnectionState struct {
	mu           sync.RWMutex
	attempts     int
	connected    bool
	lastAttempt  time.Time
	backoffDelay time.Duration
}

func (c *channelConnectionState) GetAttempts() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.attempts
}

func (c *channelConnectionState) SetAttempts(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempts = n
}

func (c *channelConnectionState) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *channelConnectionState) SetConnected(v bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected = v
}

func (c *channelConnectionState) GetLastAttempt() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lastAttempt
}

func (c *channelConnectionState) SetLastAttempt(t time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastAttempt = t
}

func (c *channelConnectionState) GetBackoffDelay() time.Duration {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.backoffDelay
}

func (c *channelConnectionState) SetBackoffDelay(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.backoffDelay = d
}

type Manager struct {
	channels      map[string]Channel
	workers       map[string]*channelWorker
	bus           *bus.MessageBus
	config        *config.Config
	mediaStore    media.MediaStore
	dispatchTask  *asyncTask
	mux           *http.ServeMux
	httpServer    *http.Server
	mu            sync.RWMutex
	placeholders  sync.Map // "channel:chatID" → placeholderID (string)
	typingStops   sync.Map // "channel:chatID" → func()
	reactionUndos sync.Map // "channel:chatID" → reactionEntry
	// Connection retry tracking
	connStates map[string]*channelConnectionState
}

type asyncTask struct {
	cancel context.CancelFunc
}

// RecordPlaceholder registers a placeholder message for later editing.
// Implements PlaceholderRecorder.
func (m *Manager) RecordPlaceholder(channel, chatID, placeholderID string) {
	key := channel + ":" + chatID
	m.placeholders.Store(key, placeholderEntry{id: placeholderID, createdAt: time.Now()})
}

// RecordTypingStop registers a typing stop function for later invocation.
// Implements PlaceholderRecorder.
func (m *Manager) RecordTypingStop(channel, chatID string, stop func()) {
	key := channel + ":" + chatID
	m.typingStops.Store(key, typingEntry{stop: stop, createdAt: time.Now()})
}

// RecordReactionUndo registers a reaction undo function for later invocation.
// Implements PlaceholderRecorder.
func (m *Manager) RecordReactionUndo(channel, chatID string, undo func()) {
	key := channel + ":" + chatID
	m.reactionUndos.Store(key, reactionEntry{undo: undo, createdAt: time.Now()})
}

// preSend handles typing stop, reaction undo, and placeholder editing before sending a message.
// Returns true if the message was edited into a placeholder (skip Send).
func (m *Manager) preSend(ctx context.Context, name string, msg bus.OutboundMessage, ch Channel) bool {
	key := name + ":" + msg.ChatID

	// 1. Stop typing
	if v, loaded := m.typingStops.LoadAndDelete(key); loaded {
		if entry, ok := v.(typingEntry); ok {
			entry.stop() // idempotent, safe
		}
	}

	// 2. Undo reaction
	if v, loaded := m.reactionUndos.LoadAndDelete(key); loaded {
		if entry, ok := v.(reactionEntry); ok {
			entry.undo() // idempotent, safe
		}
	}

	// 3. Try editing placeholder
	if v, loaded := m.placeholders.LoadAndDelete(key); loaded {
		if entry, ok := v.(placeholderEntry); ok && entry.id != "" {
			if editor, ok := ch.(MessageEditor); ok {
				if err := editor.EditMessage(ctx, msg.ChatID, entry.id, msg.Content); err == nil {
					return true // edited successfully, skip Send
				}
				// edit failed → fall through to normal Send
			}
		}
	}

	return false
}

func NewManager(cfg *config.Config, messageBus *bus.MessageBus, store media.MediaStore) (*Manager, error) {
	m := &Manager{
		channels:   make(map[string]Channel),
		workers:    make(map[string]*channelWorker),
		connStates: make(map[string]*channelConnectionState),
		bus:        messageBus,
		config:     cfg,
		mediaStore: store,
	}

	if err := m.initChannels(); err != nil {
		return nil, err
	}

	return m, nil
}

// initChannel is a helper that looks up a factory by name and creates the channel.
func (m *Manager) initChannel(name, displayName string) {
	f, ok := getFactory(name)
	if !ok {
		logger.WarnCF("channels", "Factory not registered", map[string]any{
			"channel": displayName,
		})
		return
	}
	logger.DebugCF("channels", "Attempting to initialize channel", map[string]any{
		"channel": displayName,
	})
	ch, err := f(m.config, m.bus)
	if err != nil {
		logger.ErrorCF("channels", "Failed to initialize channel", map[string]any{
			"channel": displayName,
			"error":   err.Error(),
		})
	} else {
		// Inject MediaStore if channel supports it
		if m.mediaStore != nil {
			if setter, ok := ch.(interface{ SetMediaStore(s media.MediaStore) }); ok {
				setter.SetMediaStore(m.mediaStore)
			}
		}
		// Inject PlaceholderRecorder if channel supports it
		if setter, ok := ch.(interface{ SetPlaceholderRecorder(r PlaceholderRecorder) }); ok {
			setter.SetPlaceholderRecorder(m)
		}
		// Inject owner reference so BaseChannel.HandleMessage can auto-trigger typing/reaction
		if setter, ok := ch.(interface{ SetOwner(ch Channel) }); ok {
			setter.SetOwner(ch)
		}
		m.channels[name] = ch
		logger.InfoCF("channels", "Channel enabled successfully", map[string]any{
			"channel": displayName,
		})
	}
}

func (m *Manager) initChannels() error {
	logger.InfoC("channels", "Initializing channel manager")

	if m.config.Channels.Telegram.Enabled && m.config.Channels.Telegram.Token != "" {
		m.initChannel("telegram", "Telegram")
	}

	if m.config.Channels.WhatsApp.Enabled {
		waCfg := m.config.Channels.WhatsApp
		if waCfg.UseNative {
			m.initChannel("whatsapp_native", "WhatsApp Native")
		} else if waCfg.BridgeURL != "" {
			m.initChannel("whatsapp", "WhatsApp")
		}
	}

	if m.config.Channels.Feishu.Enabled {
		m.initChannel("feishu", "Feishu")
	}

	if m.config.Channels.Discord.Enabled && m.config.Channels.Discord.Token != "" {
		m.initChannel("discord", "Discord")
	}

	if m.config.Channels.MaixCam.Enabled {
		m.initChannel("maixcam", "MaixCam")
	}

	if m.config.Channels.QQ.Enabled {
		m.initChannel("qq", "QQ")
	}

	if m.config.Channels.DingTalk.Enabled && m.config.Channels.DingTalk.ClientID != "" {
		m.initChannel("dingtalk", "DingTalk")
	}

	if m.config.Channels.Slack.Enabled && m.config.Channels.Slack.BotToken != "" {
		m.initChannel("slack", "Slack")
	}

	if m.config.Channels.LINE.Enabled && m.config.Channels.LINE.ChannelAccessToken != "" {
		m.initChannel("line", "LINE")
	}

	if m.config.Channels.OneBot.Enabled && m.config.Channels.OneBot.WSUrl != "" {
		m.initChannel("onebot", "OneBot")
	}

	if m.config.Channels.WeCom.Enabled && m.config.Channels.WeCom.Token != "" {
		m.initChannel("wecom", "WeCom")
	}

	if m.config.Channels.WeComAIBot.Enabled && m.config.Channels.WeComAIBot.Token != "" {
		m.initChannel("wecom_aibot", "WeCom AI Bot")
	}

	if m.config.Channels.WeComApp.Enabled && m.config.Channels.WeComApp.CorpID != "" {
		m.initChannel("wecom_app", "WeCom App")
	}

	if m.config.Channels.Pico.Enabled && m.config.Channels.Pico.Token != "" {
		m.initChannel("pico", "Pico")
	}

	logger.InfoCF("channels", "Channel initialization completed", map[string]any{
		"enabled_channels": len(m.channels),
	})

	return nil
}

// SetupHTTPServer creates a shared HTTP server with the given listen address.
// It registers health endpoints from the health server and discovers channels
// that implement WebhookHandler and/or HealthChecker to register their handlers.
func (m *Manager) SetupHTTPServer(addr string, healthServer *health.Server) {
	m.mux = http.NewServeMux()

	// Register health endpoints
	if healthServer != nil {
		healthServer.RegisterOnMux(m.mux)
	}

	// Discover and register webhook handlers and health checkers
	for name, ch := range m.channels {
		if wh, ok := ch.(WebhookHandler); ok {
			m.mux.Handle(wh.WebhookPath(), wh)
			logger.InfoCF("channels", "Webhook handler registered", map[string]any{
				"channel": name,
				"path":    wh.WebhookPath(),
			})
		}
		if hc, ok := ch.(HealthChecker); ok {
			m.mux.HandleFunc(hc.HealthPath(), hc.HealthHandler)
			logger.InfoCF("channels", "Health endpoint registered", map[string]any{
				"channel": name,
				"path":    hc.HealthPath(),
			})
		}
	}

	m.httpServer = &http.Server{
		Addr:         addr,
		Handler:      m.mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}
}

func (m *Manager) StartAll(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.channels) == 0 {
		logger.WarnC("channels", "No channels enabled")
		return errors.New("no channels enabled")
	}

	logger.InfoC("channels", "Starting all channels with retry logic")

	dispatchCtx, cancel := context.WithCancel(ctx)
	m.dispatchTask = &asyncTask{cancel: cancel}

	// Initialize connection states for all channels
	for name := range m.channels {
		m.connStates[name] = &channelConnectionState{
			attempts:     0,
			connected:    false,
			backoffDelay: defaultBaseBackoffDelay,
		}
	}

	// Retry loop for starting channels
	for {
		allConnected := true
		allRetriesExhausted := true
		channelsAttempted := 0

		for name, channel := range m.channels {
			state := m.connStates[name]

			// Skip already connected channels
			if state.IsConnected() {
				continue
			}

			// Check if max retries exceeded for this channel
			maxRetries := m.getMaxRetries(name)
			attempts := state.GetAttempts()
			if maxRetries > 0 && attempts >= maxRetries {
				// Skip this channel, it's exhausted its retries
				logger.WarnCF("channels", "Channel has exhausted all retry attempts", map[string]any{
					"channel":     name,
					"attempts":    attempts,
					"max_retries": maxRetries,
				})
				continue
			}

			// This channel still needs connection
			allConnected = false
			allRetriesExhausted = false
			channelsAttempted++

			// Attempt to start the channel
			state.SetAttempts(attempts + 1)
			state.SetLastAttempt(time.Now())

			logger.InfoCF("channels", "Attempting to start channel", map[string]any{
				"channel": name,
				"attempt": attempts + 1,
			})

			if err := channel.Start(ctx); err != nil {
				logger.ErrorCF("channels", "Failed to start channel", map[string]any{
					"channel": name,
					"attempt": attempts + 1,
					"error":   err.Error(),
				})

				// Calculate next backoff delay with exponential backoff
				backoffFactor := m.getBackoffFactor(name)
				currentBackoff := state.GetBackoffDelay()
				newBackoff := time.Duration(float64(currentBackoff) * backoffFactor)
				if newBackoff > defaultMaxBackoffDelay {
					newBackoff = defaultMaxBackoffDelay
				}
				state.SetBackoffDelay(newBackoff)
			} else {
				// Channel started successfully
				state.SetConnected(true)
				logger.InfoCF("channels", "Channel started successfully", map[string]any{
					"channel":  name,
					"attempts": attempts + 1,
				})

				// Create worker for this channel
				w := newChannelWorker(name, channel)
				m.workers[name] = w
				go m.runWorker(dispatchCtx, name, w)
				go m.runMediaWorker(dispatchCtx, name, w)

				// Check if all channels are now connected - if so, break early
				allConnected = true
				for _, s := range m.connStates {
					if !s.IsConnected() {
						allConnected = false
						break
					}
				}
				if allConnected {
					break
				}
			}
		}

		// Check if we should stop retrying
		if allConnected {
			logger.InfoC("channels", "All channels connected successfully")
			break
		}

		if allRetriesExhausted {
			logger.WarnC("channels", "All channels have exhausted their retry attempts")
			break
		}

		// Wait for the minimum backoff delay before next iteration
		// Find the minimum backoff among channels that still need connection
		minBackoff := defaultMaxBackoffDelay
		channelsNeedingRetry := 0
		for name, state := range m.connStates {
			if !state.IsConnected() {
				maxRetries := m.getMaxRetries(name)
				attempts := state.GetAttempts()
				if maxRetries == 0 || attempts < maxRetries {
					backoff := state.GetBackoffDelay()
					if backoff < minBackoff {
						minBackoff = backoff
					}
					channelsNeedingRetry++
				}
			}
		}

		// If no channels need retry (all exhausted), exit now
		if channelsNeedingRetry == 0 {
			logger.WarnC("channels", "All channels have exhausted their retry attempts")
			break
		}

		logger.InfoCF("channels", "Waiting before next connection attempt", map[string]any{
			"channels": channelsAttempted,
			"delay":    minBackoff.String(),
		})

		select {
		case <-time.After(minBackoff):
			// Continue to next iteration
		case <-ctx.Done():
			logger.WarnC("channels", "Context cancelled, stopping channel startup retry loop")
			return ctx.Err()
		}
	}

	// Start the dispatcher that reads from the bus and routes to workers
	go m.dispatchOutbound(dispatchCtx)
	go m.dispatchOutboundMedia(dispatchCtx)

	// Start the TTL janitor that cleans up stale typing/placeholder entries
	go m.runTTLJanitor(dispatchCtx)

	// Start shared HTTP server if configured
	if m.httpServer != nil {
		go func() {
			logger.InfoCF("channels", "Shared HTTP server listening", map[string]any{
				"addr": m.httpServer.Addr,
			})
			if err := m.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				logger.ErrorCF("channels", "Shared HTTP server error", map[string]any{
					"error": err.Error(),
				})
			}
		}()
	}

	// Log connection status summary
	connectedCount := 0
	failedCount := 0
	for _, state := range m.connStates {
		if state.IsConnected() {
			connectedCount++
		} else {
			failedCount++
		}
	}

	logger.InfoCF("channels", "Channel startup completed", map[string]any{
		"connected": connectedCount,
		"failed":    failedCount,
		"total":     len(m.channels),
	})

	return nil
}

// getMaxRetries returns the max retry count for a specific channel
func (m *Manager) getMaxRetries(channelName string) int {
	switch channelName {
	case "whatsapp":
		return m.config.Channels.WhatsApp.MaxRetries
	case "telegram":
		return m.config.Channels.Telegram.MaxRetries
	case "feishu":
		return m.config.Channels.Feishu.MaxRetries
	case "discord":
		return m.config.Channels.Discord.MaxRetries
	case "maixcam":
		return m.config.Channels.MaixCam.MaxRetries
	case "qq":
		return m.config.Channels.QQ.MaxRetries
	case "dingtalk":
		return m.config.Channels.DingTalk.MaxRetries
	case "slack":
		return m.config.Channels.Slack.MaxRetries
	case "line":
		return m.config.Channels.LINE.MaxRetries
	case "onebot":
		return m.config.Channels.OneBot.MaxRetries
	case "wecom":
		return m.config.Channels.WeCom.MaxRetries
	case "wecom_app":
		return m.config.Channels.WeComApp.MaxRetries
	case "wecom_aibot":
		return m.config.Channels.WeComAIBot.MaxRetries
	case "pico":
		return m.config.Channels.Pico.MaxRetries
	default:
		return 0
	}
}

// getBackoffFactor returns the backoff factor for a specific channel
func (m *Manager) getBackoffFactor(channelName string) float64 {
	switch channelName {
	case "whatsapp":
		if m.config.Channels.WhatsApp.BackoffFactor > 0 {
			return m.config.Channels.WhatsApp.BackoffFactor
		}
	case "telegram":
		if m.config.Channels.Telegram.BackoffFactor > 0 {
			return m.config.Channels.Telegram.BackoffFactor
		}
	case "feishu":
		if m.config.Channels.Feishu.BackoffFactor > 0 {
			return m.config.Channels.Feishu.BackoffFactor
		}
	case "discord":
		if m.config.Channels.Discord.BackoffFactor > 0 {
			return m.config.Channels.Discord.BackoffFactor
		}
	case "maixcam":
		if m.config.Channels.MaixCam.BackoffFactor > 0 {
			return m.config.Channels.MaixCam.BackoffFactor
		}
	case "qq":
		if m.config.Channels.QQ.BackoffFactor > 0 {
			return m.config.Channels.QQ.BackoffFactor
		}
	case "dingtalk":
		if m.config.Channels.DingTalk.BackoffFactor > 0 {
			return m.config.Channels.DingTalk.BackoffFactor
		}
	case "slack":
		if m.config.Channels.Slack.BackoffFactor > 0 {
			return m.config.Channels.Slack.BackoffFactor
		}
	case "line":
		if m.config.Channels.LINE.BackoffFactor > 0 {
			return m.config.Channels.LINE.BackoffFactor
		}
	case "onebot":
		if m.config.Channels.OneBot.BackoffFactor > 0 {
			return m.config.Channels.OneBot.BackoffFactor
		}
	case "wecom":
		if m.config.Channels.WeCom.BackoffFactor > 0 {
			return m.config.Channels.WeCom.BackoffFactor
		}
	case "wecom_app":
		if m.config.Channels.WeComApp.BackoffFactor > 0 {
			return m.config.Channels.WeComApp.BackoffFactor
		}
	case "wecom_aibot":
		if m.config.Channels.WeComAIBot.BackoffFactor > 0 {
			return m.config.Channels.WeComAIBot.BackoffFactor
		}
	case "pico":
		if m.config.Channels.Pico.BackoffFactor > 0 {
			return m.config.Channels.Pico.BackoffFactor
		}
	}
	return defaultBackoffFactor
}

func (m *Manager) StopAll(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	logger.InfoC("channels", "Stopping all channels")

	// Shutdown shared HTTP server first
	if m.httpServer != nil {
		shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if err := m.httpServer.Shutdown(shutdownCtx); err != nil {
			logger.ErrorCF("channels", "Shared HTTP server shutdown error", map[string]any{
				"error": err.Error(),
			})
		}
		m.httpServer = nil
	}

	// Cancel dispatcher
	if m.dispatchTask != nil {
		m.dispatchTask.cancel()
		m.dispatchTask = nil
	}

	// Close all worker queues and wait for them to drain
	for _, w := range m.workers {
		if w != nil {
			close(w.queue)
		}
	}
	for _, w := range m.workers {
		if w != nil {
			<-w.done
		}
	}
	// Close all media worker queues and wait for them to drain
	for _, w := range m.workers {
		if w != nil {
			close(w.mediaQueue)
		}
	}
	for _, w := range m.workers {
		if w != nil {
			<-w.mediaDone
		}
	}

	// Stop all channels
	for name, channel := range m.channels {
		logger.InfoCF("channels", "Stopping channel", map[string]any{
			"channel": name,
		})
		if err := channel.Stop(ctx); err != nil {
			logger.ErrorCF("channels", "Error stopping channel", map[string]any{
				"channel": name,
				"error":   err.Error(),
			})
		}
	}

	logger.InfoC("channels", "All channels stopped")
	return nil
}

// newChannelWorker creates a channelWorker with a rate limiter configured
// for the given channel name.
func newChannelWorker(name string, ch Channel) *channelWorker {
	rateVal := float64(defaultRateLimit)
	if r, ok := channelRateConfig[name]; ok {
		rateVal = r
	}
	burst := int(math.Max(1, math.Ceil(rateVal/2)))

	return &channelWorker{
		ch:         ch,
		queue:      make(chan bus.OutboundMessage, defaultChannelQueueSize),
		mediaQueue: make(chan bus.OutboundMediaMessage, defaultChannelQueueSize),
		done:       make(chan struct{}),
		mediaDone:  make(chan struct{}),
		limiter:    rate.NewLimiter(rate.Limit(rateVal), burst),
	}
}

// runWorker processes outbound messages for a single channel, splitting
// messages that exceed the channel's maximum message length.
func (m *Manager) runWorker(ctx context.Context, name string, w *channelWorker) {
	defer close(w.done)
	for {
		select {
		case msg, ok := <-w.queue:
			if !ok {
				return
			}
			maxLen := 0
			if mlp, ok := w.ch.(MessageLengthProvider); ok {
				maxLen = mlp.MaxMessageLength()
			}
			if maxLen > 0 && len([]rune(msg.Content)) > maxLen {
				chunks := SplitMessage(msg.Content, maxLen)
				for _, chunk := range chunks {
					chunkMsg := msg
					chunkMsg.Content = chunk
					m.sendWithRetry(ctx, name, w, chunkMsg)
				}
			} else {
				m.sendWithRetry(ctx, name, w, msg)
			}
		case <-ctx.Done():
			return
		}
	}
}

// sendWithRetry sends a message through the channel with rate limiting and
// retry logic. It classifies errors to determine the retry strategy:
//   - ErrNotRunning / ErrSendFailed: permanent, no retry
//   - ErrRateLimit: fixed delay retry
//   - ErrTemporary / unknown: exponential backoff retry
func (m *Manager) sendWithRetry(ctx context.Context, name string, w *channelWorker, msg bus.OutboundMessage) {
	// Rate limit: wait for token
	if err := w.limiter.Wait(ctx); err != nil {
		// ctx canceled, shutting down
		return
	}

	// Pre-send: stop typing and try to edit placeholder
	if m.preSend(ctx, name, msg, w.ch) {
		return // placeholder was edited successfully, skip Send
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		lastErr = w.ch.Send(ctx, msg)
		if lastErr == nil {
			return
		}

		// Permanent failures — don't retry
		if errors.Is(lastErr, ErrNotRunning) || errors.Is(lastErr, ErrSendFailed) {
			break
		}

		// Last attempt exhausted — don't sleep
		if attempt == maxRetries {
			break
		}

		// Rate limit error — fixed delay
		if errors.Is(lastErr, ErrRateLimit) {
			select {
			case <-time.After(rateLimitDelay):
				continue
			case <-ctx.Done():
				return
			}
		}

		// ErrTemporary or unknown error — exponential backoff
		backoff := min(time.Duration(float64(baseBackoff)*math.Pow(2, float64(attempt))), maxBackoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
	}

	// All retries exhausted or permanent failure
	logger.ErrorCF("channels", "Send failed", map[string]any{
		"channel": name,
		"chat_id": msg.ChatID,
		"error":   lastErr.Error(),
		"retries": maxRetries,
	})
}

func dispatchLoop[M any](
	ctx context.Context,
	m *Manager,
	subscribe func(context.Context) (M, bool),
	getChannel func(M) string,
	enqueue func(context.Context, *channelWorker, M) bool,
	startMsg, stopMsg, unknownMsg, noWorkerMsg string,
) {
	logger.InfoC("channels", startMsg)

	for {
		msg, ok := subscribe(ctx)
		if !ok {
			logger.InfoC("channels", stopMsg)
			return
		}

		channel := getChannel(msg)

		// Silently skip internal channels
		if constants.IsInternalChannel(channel) {
			continue
		}

		m.mu.RLock()
		_, exists := m.channels[channel]
		w, wExists := m.workers[channel]
		m.mu.RUnlock()

		if !exists {
			logger.WarnCF("channels", unknownMsg, map[string]any{"channel": channel})
			continue
		}

		if wExists && w != nil {
			if !enqueue(ctx, w, msg) {
				return
			}
		} else if exists {
			logger.WarnCF("channels", noWorkerMsg, map[string]any{"channel": channel})
		}
	}
}

func (m *Manager) dispatchOutbound(ctx context.Context) {
	dispatchLoop(
		ctx, m,
		m.bus.SubscribeOutbound,
		func(msg bus.OutboundMessage) string { return msg.Channel },
		func(ctx context.Context, w *channelWorker, msg bus.OutboundMessage) bool {
			select {
			case w.queue <- msg:
				return true
			case <-ctx.Done():
				return false
			}
		},
		"Outbound dispatcher started",
		"Outbound dispatcher stopped",
		"Unknown channel for outbound message",
		"Channel has no active worker, skipping message",
	)
}

func (m *Manager) dispatchOutboundMedia(ctx context.Context) {
	dispatchLoop(
		ctx, m,
		m.bus.SubscribeOutboundMedia,
		func(msg bus.OutboundMediaMessage) string { return msg.Channel },
		func(ctx context.Context, w *channelWorker, msg bus.OutboundMediaMessage) bool {
			select {
			case w.mediaQueue <- msg:
				return true
			case <-ctx.Done():
				return false
			}
		},
		"Outbound media dispatcher started",
		"Outbound media dispatcher stopped",
		"Unknown channel for outbound media message",
		"Channel has no active worker, skipping media message",
	)
}

// runMediaWorker processes outbound media messages for a single channel.
func (m *Manager) runMediaWorker(ctx context.Context, name string, w *channelWorker) {
	defer close(w.mediaDone)
	for {
		select {
		case msg, ok := <-w.mediaQueue:
			if !ok {
				return
			}
			m.sendMediaWithRetry(ctx, name, w, msg)
		case <-ctx.Done():
			return
		}
	}
}

// sendMediaWithRetry sends a media message through the channel with rate limiting and
// retry logic. If the channel does not implement MediaSender, it silently skips.
func (m *Manager) sendMediaWithRetry(ctx context.Context, name string, w *channelWorker, msg bus.OutboundMediaMessage) {
	ms, ok := w.ch.(MediaSender)
	if !ok {
		logger.DebugCF("channels", "Channel does not support MediaSender, skipping media", map[string]any{
			"channel": name,
		})
		return
	}

	// Rate limit: wait for token
	if err := w.limiter.Wait(ctx); err != nil {
		return
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		lastErr = ms.SendMedia(ctx, msg)
		if lastErr == nil {
			return
		}

		// Permanent failures — don't retry
		if errors.Is(lastErr, ErrNotRunning) || errors.Is(lastErr, ErrSendFailed) {
			break
		}

		// Last attempt exhausted — don't sleep
		if attempt == maxRetries {
			break
		}

		// Rate limit error — fixed delay
		if errors.Is(lastErr, ErrRateLimit) {
			select {
			case <-time.After(rateLimitDelay):
				continue
			case <-ctx.Done():
				return
			}
		}

		// ErrTemporary or unknown error — exponential backoff
		backoff := min(time.Duration(float64(baseBackoff)*math.Pow(2, float64(attempt))), maxBackoff)
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return
		}
	}

	// All retries exhausted or permanent failure
	logger.ErrorCF("channels", "SendMedia failed", map[string]any{
		"channel": name,
		"chat_id": msg.ChatID,
		"error":   lastErr.Error(),
		"retries": maxRetries,
	})
}

// runTTLJanitor periodically scans the typingStops and placeholders maps
// and evicts entries that have exceeded their TTL. This prevents memory
// accumulation when outbound paths fail to trigger preSend (e.g. LLM errors).
func (m *Manager) runTTLJanitor(ctx context.Context) {
	ticker := time.NewTicker(janitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			m.typingStops.Range(func(key, value any) bool {
				if entry, ok := value.(typingEntry); ok {
					if now.Sub(entry.createdAt) > typingStopTTL {
						if _, loaded := m.typingStops.LoadAndDelete(key); loaded {
							entry.stop() // idempotent, safe
						}
					}
				}
				return true
			})
			m.reactionUndos.Range(func(key, value any) bool {
				if entry, ok := value.(reactionEntry); ok {
					if now.Sub(entry.createdAt) > typingStopTTL {
						if _, loaded := m.reactionUndos.LoadAndDelete(key); loaded {
							entry.undo() // idempotent, safe
						}
					}
				}
				return true
			})
			m.placeholders.Range(func(key, value any) bool {
				if entry, ok := value.(placeholderEntry); ok {
					if now.Sub(entry.createdAt) > placeholderTTL {
						m.placeholders.Delete(key)
					}
				}
				return true
			})
		}
	}
}

func (m *Manager) GetChannel(name string) (Channel, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	channel, ok := m.channels[name]
	return channel, ok
}

func (m *Manager) GetStatus() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make(map[string]any)
	for name, channel := range m.channels {
		state := m.connStates[name]
		channelStatus := map[string]any{
			"enabled": true,
			"running": channel.IsRunning(),
		}
		if state != nil {
			channelStatus["connected"] = state.IsConnected()
			channelStatus["attempts"] = state.GetAttempts()
			channelStatus["max_retries"] = m.getMaxRetries(name)
		}
		status[name] = channelStatus
	}
	return status
}

// GetConnectionStatus returns detailed connection status for all channels
func (m *Manager) GetConnectionStatus() map[string]any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	status := make(map[string]any)
	connectedCount := 0
	failedCount := 0

	for name, state := range m.connStates {
		channel := m.channels[name]
		isRunning := false
		if channel != nil {
			isRunning = channel.IsRunning()
		}

		status[name] = map[string]any{
			"connected":    state.IsConnected(),
			"attempts":     state.GetAttempts(),
			"max_retries":  m.getMaxRetries(name),
			"running":      isRunning,
			"last_attempt": state.GetLastAttempt(),
		}

		if state.IsConnected() {
			connectedCount++
		} else {
			failedCount++
		}
	}

	return map[string]any{
		"channels":  status,
		"connected": connectedCount,
		"failed":    failedCount,
		"total":     len(m.connStates),
	}
}

func (m *Manager) GetEnabledChannels() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	names := make([]string, 0, len(m.channels))
	for name := range m.channels {
		names = append(names, name)
	}
	return names
}

func (m *Manager) RegisterChannel(name string, channel Channel) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.channels[name] = channel
}

func (m *Manager) UnregisterChannel(name string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if w, ok := m.workers[name]; ok && w != nil {
		close(w.queue)
		<-w.done
		close(w.mediaQueue)
		<-w.mediaDone
	}
	delete(m.workers, name)
	delete(m.channels, name)
	delete(m.connStates, name)
}

func (m *Manager) SendToChannel(ctx context.Context, channelName, chatID, content string) error {
	m.mu.RLock()
	_, exists := m.channels[channelName]
	w, wExists := m.workers[channelName]
	m.mu.RUnlock()

	if !exists {
		return fmt.Errorf("channel %s not found", channelName)
	}

	msg := bus.OutboundMessage{
		Channel: channelName,
		ChatID:  chatID,
		Content: content,
	}

	if wExists && w != nil {
		select {
		case w.queue <- msg:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Fallback: direct send (should not happen)
	channel, _ := m.channels[channelName]
	return channel.Send(ctx, msg)
}
