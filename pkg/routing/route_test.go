package routing

import (
	"testing"

	"github.com/sipeed/picoclaw/pkg/config"
)

func testConfig(agents map[string]config.AgentConfig, bindings []config.AgentBinding) *config.Config {
	return &config.Config{
		Agents: config.AgentsConfig{
			Defaults: config.AgentDefaults{
				Workspace: "/tmp/picoclaw-test",
				Model:     "gpt-4",
			},
			Agents: agents,
		},
		Bindings: bindings,
		Session: config.SessionConfig{
			DMScope: "per-peer",
		},
	}
}

func TestResolveRoute_DefaultAgent_NoBindings(t *testing.T) {
	cfg := testConfig(nil, nil)
	r := NewRouteResolver(cfg)

	route := r.ResolveRoute(RouteInput{
		Channel: "telegram",
		Peer:    &RoutePeer{Kind: "direct", ID: "user1"},
	})

	if route.AgentID != DefaultAgentID {
		t.Errorf("AgentID = %q, want %q", route.AgentID, DefaultAgentID)
	}
	if route.MatchedBy != "default" {
		t.Errorf("MatchedBy = %q, want 'default'", route.MatchedBy)
	}
}

func TestResolveRoute_PeerBinding(t *testing.T) {
	agents := map[string]config.AgentConfig{
		"sales":   {},
		"support": {},
	}
	bindings := []config.AgentBinding{
		{
			AgentID: "support",
			Match: config.BindingMatch{
				Channel:   "telegram",
				AccountID: "*",
				Peer:      &config.PeerMatch{Kind: "direct", ID: "user123"},
			},
		},
	}
	cfg := testConfig(agents, bindings)
	r := NewRouteResolver(cfg)

	route := r.ResolveRoute(RouteInput{
		Channel: "telegram",
		Peer:    &RoutePeer{Kind: "direct", ID: "user123"},
	})

	if route.AgentID != "support" {
		t.Errorf("AgentID = %q, want 'support'", route.AgentID)
	}
	if route.MatchedBy != "binding.peer" {
		t.Errorf("MatchedBy = %q, want 'binding.peer'", route.MatchedBy)
	}
}

func TestResolveRoute_GuildBinding(t *testing.T) {
	agents := map[string]config.AgentConfig{
		"general": {},
		"gaming":  {},
	}
	bindings := []config.AgentBinding{
		{
			AgentID: "gaming",
			Match: config.BindingMatch{
				Channel:   "discord",
				AccountID: "*",
				GuildID:   "guild-abc",
			},
		},
	}
	cfg := testConfig(agents, bindings)
	r := NewRouteResolver(cfg)

	route := r.ResolveRoute(RouteInput{
		Channel: "discord",
		GuildID: "guild-abc",
		Peer:    &RoutePeer{Kind: "channel", ID: "ch1"},
	})

	if route.AgentID != "gaming" {
		t.Errorf("AgentID = %q, want 'gaming'", route.AgentID)
	}
	if route.MatchedBy != "binding.guild" {
		t.Errorf("MatchedBy = %q, want 'binding.guild'", route.MatchedBy)
	}
}

func TestResolveRoute_TeamBinding(t *testing.T) {
	agents := map[string]config.AgentConfig{
		"general": {},
		"work":    {},
	}
	bindings := []config.AgentBinding{
		{
			AgentID: "work",
			Match: config.BindingMatch{
				Channel:   "slack",
				AccountID: "*",
				TeamID:    "T12345",
			},
		},
	}
	cfg := testConfig(agents, bindings)
	r := NewRouteResolver(cfg)

	route := r.ResolveRoute(RouteInput{
		Channel: "slack",
		TeamID:  "T12345",
		Peer:    &RoutePeer{Kind: "channel", ID: "C001"},
	})

	if route.AgentID != "work" {
		t.Errorf("AgentID = %q, want 'work'", route.AgentID)
	}
	if route.MatchedBy != "binding.team" {
		t.Errorf("MatchedBy = %q, want 'binding.team'", route.MatchedBy)
	}
}

func TestResolveRoute_AccountBinding(t *testing.T) {
	agents := map[string]config.AgentConfig{
		"default-agent": {},
		"premium":       {},
	}
	bindings := []config.AgentBinding{
		{
			AgentID: "premium",
			Match: config.BindingMatch{
				Channel:   "telegram",
				AccountID: "bot2",
			},
		},
	}
	cfg := testConfig(agents, bindings)
	r := NewRouteResolver(cfg)

	route := r.ResolveRoute(RouteInput{
		Channel:   "telegram",
		AccountID: "bot2",
		Peer:      &RoutePeer{Kind: "direct", ID: "user1"},
	})

	if route.AgentID != "premium" {
		t.Errorf("AgentID = %q, want 'premium'", route.AgentID)
	}
	if route.MatchedBy != "binding.account" {
		t.Errorf("MatchedBy = %q, want 'binding.account'", route.MatchedBy)
	}
}

func TestResolveRoute_ChannelWildcard(t *testing.T) {
	agents := map[string]config.AgentConfig{
		"main":         {},
		"telegram-bot": {},
	}
	bindings := []config.AgentBinding{
		{
			AgentID: "telegram-bot",
			Match: config.BindingMatch{
				Channel:   "telegram",
				AccountID: "*",
			},
		},
	}
	cfg := testConfig(agents, bindings)
	r := NewRouteResolver(cfg)

	route := r.ResolveRoute(RouteInput{
		Channel: "telegram",
		Peer:    &RoutePeer{Kind: "direct", ID: "user1"},
	})

	if route.AgentID != "telegram-bot" {
		t.Errorf("AgentID = %q, want 'telegram-bot'", route.AgentID)
	}
	if route.MatchedBy != "binding.channel" {
		t.Errorf("MatchedBy = %q, want 'binding.channel'", route.MatchedBy)
	}
}

func TestResolveRoute_PriorityOrder_PeerBeatsGuild(t *testing.T) {
	agents := map[string]config.AgentConfig{
		"general": {},
		"vip":     {},
		"gaming":  {},
	}
	bindings := []config.AgentBinding{
		{
			AgentID: "vip",
			Match: config.BindingMatch{
				Channel:   "discord",
				AccountID: "*",
				Peer:      &config.PeerMatch{Kind: "direct", ID: "user-vip"},
			},
		},
		{
			AgentID: "gaming",
			Match: config.BindingMatch{
				Channel:   "discord",
				AccountID: "*",
				GuildID:   "guild-1",
			},
		},
	}
	cfg := testConfig(agents, bindings)
	r := NewRouteResolver(cfg)

	route := r.ResolveRoute(RouteInput{
		Channel: "discord",
		GuildID: "guild-1",
		Peer:    &RoutePeer{Kind: "direct", ID: "user-vip"},
	})

	if route.AgentID != "vip" {
		t.Errorf("AgentID = %q, want 'vip' (peer should beat guild)", route.AgentID)
	}
	if route.MatchedBy != "binding.peer" {
		t.Errorf("MatchedBy = %q, want 'binding.peer'", route.MatchedBy)
	}
}

func TestResolveRoute_InvalidAgentFallsToDefault(t *testing.T) {
	agents := map[string]config.AgentConfig{
		"main": {},
	}
	bindings := []config.AgentBinding{
		{
			AgentID: "nonexistent",
			Match: config.BindingMatch{
				Channel:   "telegram",
				AccountID: "*",
			},
		},
	}
	cfg := testConfig(agents, bindings)
	r := NewRouteResolver(cfg)

	route := r.ResolveRoute(RouteInput{
		Channel: "telegram",
	})

	if route.AgentID != "main" {
		t.Errorf("AgentID = %q, want 'main' (invalid agent should fall to default)", route.AgentID)
	}
}

func TestResolveRoute_DefaultAgentSelection(t *testing.T) {
	agents := map[string]config.AgentConfig{
		"alpha": {},
		"beta":  {},
		"gamma": {},
	}
	cfg := testConfig(agents, nil)
	r := NewRouteResolver(cfg)

	route := r.ResolveRoute(RouteInput{
		Channel: "cli",
	})

	// With new behavior, agents.defaults is always the default
	if route.AgentID != DefaultAgentID {
		t.Errorf("AgentID = %q, want %q (agents.defaults is always default)", route.AgentID, DefaultAgentID)
	}
}

func TestResolveRoute_UsesNamedAgentWithFlag(t *testing.T) {
	agents := map[string]config.AgentConfig{
		"alpha": {},
		"beta":  {},
	}
	cfg := testConfig(agents, nil)
	r := NewRouteResolver(cfg)

	// Use a binding to route to a named agent
	binding := []config.AgentBinding{
		{
			AgentID: "beta",
			Match: config.BindingMatch{
				Channel:   "cli",
				AccountID: "*",
			},
		},
	}
	cfg.Bindings = binding

	route := r.ResolveRoute(RouteInput{
		Channel: "cli",
	})

	if route.AgentID != "beta" {
		t.Errorf("AgentID = %q, want 'beta' (matched by binding)", route.AgentID)
	}
}
