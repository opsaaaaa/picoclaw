package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestAgentModelConfig_UnmarshalString(t *testing.T) {
	var m AgentModelConfig
	if err := json.Unmarshal([]byte(`"gpt-4"`), &m); err != nil {
		t.Fatalf("unmarshal string: %v", err)
	}
	if m.Primary != "gpt-4" {
		t.Errorf("Primary = %q, want 'gpt-4'", m.Primary)
	}
	if m.Fallbacks != nil {
		t.Errorf("Fallbacks = %v, want nil", m.Fallbacks)
	}
}

func TestAgentModelConfig_UnmarshalObject(t *testing.T) {
	var m AgentModelConfig
	data := `{"primary": "claude-opus", "fallbacks": ["gpt-4o-mini", "haiku"]}`
	if err := json.Unmarshal([]byte(data), &m); err != nil {
		t.Fatalf("unmarshal object: %v", err)
	}
	if m.Primary != "claude-opus" {
		t.Errorf("Primary = %q, want 'claude-opus'", m.Primary)
	}
	if len(m.Fallbacks) != 2 {
		t.Fatalf("Fallbacks len = %d, want 2", len(m.Fallbacks))
	}
	if m.Fallbacks[0] != "gpt-4o-mini" || m.Fallbacks[1] != "haiku" {
		t.Errorf("Fallbacks = %v", m.Fallbacks)
	}
}

func TestAgentModelConfig_MarshalString(t *testing.T) {
	m := AgentModelConfig{Primary: "gpt-4"}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if string(data) != `"gpt-4"` {
		t.Errorf("marshal = %s, want '\"gpt-4\"'", string(data))
	}
}

func TestAgentModelConfig_MarshalObject(t *testing.T) {
	m := AgentModelConfig{Primary: "claude-opus", Fallbacks: []string{"haiku"}}
	data, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var result map[string]any
	json.Unmarshal(data, &result)
	if result["primary"] != "claude-opus" {
		t.Errorf("primary = %v", result["primary"])
	}
}

func TestAgentConfig_FullParse(t *testing.T) {
	jsonData := `{
		"agents": {
			"defaults": {
				"workspace": "~/.picoclaw/workspace",
				"model": "glm-4.7",
				"max_tokens": 8192,
				"max_tool_iterations": 20
			},
			"sales": {
				"name": "Sales Bot",
				"model": "gpt-4"
			},
			"support": {
				"name": "Support Bot",
				"model": {
					"primary": "claude-opus",
					"fallbacks": ["haiku"]
				},
				"subagents": {
					"allow_agents": ["sales"]
				}
			}
		},
		"bindings": [
			{
				"agent_id": "support",
				"match": {
					"channel": "telegram",
					"account_id": "*",
					"peer": {"kind": "direct", "id": "user123"}
				}
			}
		],
		"session": {
			"dm_scope": "per-peer",
			"identity_links": {
				"john": ["telegram:123", "discord:john#1234"]
			}
		}
	}`

	cfg := DefaultConfig()
	if err := json.Unmarshal([]byte(jsonData), cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(cfg.Agents.Agents) != 2 {
		t.Fatalf("agents map len = %d, want 2", len(cfg.Agents.Agents))
	}

	sales, ok := cfg.Agents.Agents["sales"]
	if !ok {
		t.Fatalf("agent 'sales' not found in agents map")
	}
	if sales.ID != "sales" || sales.Name != "Sales Bot" {
		t.Errorf("sales = %+v", sales)
	}
	if sales.Model == nil || sales.Model.Primary != "gpt-4" {
		t.Errorf("sales.Model = %+v", sales.Model)
	}

	support, ok := cfg.Agents.Agents["support"]
	if !ok {
		t.Fatalf("agent 'support' not found in agents map")
	}
	if support.ID != "support" || support.Name != "Support Bot" {
		t.Errorf("support = %+v", support)
	}
	if support.Model == nil || support.Model.Primary != "claude-opus" {
		t.Errorf("support.Model = %+v", support.Model)
	}
	if len(support.Model.Fallbacks) != 1 || support.Model.Fallbacks[0] != "haiku" {
		t.Errorf("support.Model.Fallbacks = %v", support.Model.Fallbacks)
	}
	if support.Subagents == nil || len(support.Subagents.AllowAgents) != 1 {
		t.Errorf("support.Subagents = %+v", support.Subagents)
	}

	if len(cfg.Bindings) != 1 {
		t.Fatalf("bindings len = %d, want 1", len(cfg.Bindings))
	}
	binding := cfg.Bindings[0]
	if binding.AgentID != "support" || binding.Match.Channel != "telegram" {
		t.Errorf("binding = %+v", binding)
	}
	if binding.Match.Peer == nil || binding.Match.Peer.Kind != "direct" || binding.Match.Peer.ID != "user123" {
		t.Errorf("binding.Match.Peer = %+v", binding.Match.Peer)
	}

	if cfg.Session.DMScope != "per-peer" {
		t.Errorf("Session.DMScope = %q", cfg.Session.DMScope)
	}
	if len(cfg.Session.IdentityLinks) != 1 {
		t.Errorf("Session.IdentityLinks = %v", cfg.Session.IdentityLinks)
	}
	links := cfg.Session.IdentityLinks["john"]
	if len(links) != 2 {
		t.Errorf("john links = %v", links)
	}
}

func TestConfig_BackwardCompat_NoAgentsList(t *testing.T) {
	jsonData := `{
		"agents": {
			"defaults": {
				"workspace": "~/.picoclaw/workspace",
				"model": "glm-4.7",
				"max_tokens": 8192,
				"max_tool_iterations": 20
			}
		}
	}`

	cfg := DefaultConfig()
	if err := json.Unmarshal([]byte(jsonData), cfg); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if len(cfg.Agents.Agents) != 0 {
		t.Errorf("agents map should be empty when no agents defined, got %d", len(cfg.Agents.Agents))
	}
	if len(cfg.Bindings) != 0 {
		t.Errorf("bindings should be empty, got %d", len(cfg.Bindings))
	}
}

// TestDefaultConfig_HeartbeatEnabled verifies heartbeat is enabled by default
func TestDefaultConfig_HeartbeatEnabled(t *testing.T) {
	cfg := DefaultConfig()

	if !cfg.Heartbeat.Enabled {
		t.Error("Heartbeat should be enabled by default")
	}
}

// TestDefaultConfig_WorkspacePath verifies workspace path is correctly set
func TestDefaultConfig_WorkspacePath(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Agents.Defaults.Workspace == "" {
		t.Error("Workspace should not be empty")
	}
}

// TestDefaultConfig_Model verifies model is set
func TestDefaultConfig_Model(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Agents.Defaults.Model != "" {
		t.Error("Model should be empty")
	}
}

// TestDefaultConfig_MaxTokens verifies max tokens has default value
func TestDefaultConfig_MaxTokens(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Agents.Defaults.MaxTokens == 0 {
		t.Error("MaxTokens should not be zero")
	}
}

// TestDefaultConfig_MaxToolIterations verifies max tool iterations has default value
func TestDefaultConfig_MaxToolIterations(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Agents.Defaults.MaxToolIterations == 0 {
		t.Error("MaxToolIterations should not be zero")
	}
}

// TestDefaultConfig_Temperature verifies temperature has default value
func TestDefaultConfig_Temperature(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Agents.Defaults.Temperature != nil {
		t.Error("Temperature should be nil when not provided")
	}
}

// TestDefaultConfig_Gateway verifies gateway defaults
func TestDefaultConfig_Gateway(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Gateway.Host != "127.0.0.1" {
		t.Error("Gateway host should have default value")
	}
	if cfg.Gateway.Port == 0 {
		t.Error("Gateway port should have default value")
	}
}

// TestDefaultConfig_Providers verifies provider structure
func TestDefaultConfig_Providers(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Providers.Anthropic.APIKey != "" {
		t.Error("Anthropic API key should be empty by default")
	}
	if cfg.Providers.OpenAI.APIKey != "" {
		t.Error("OpenAI API key should be empty by default")
	}
	if cfg.Providers.OpenRouter.APIKey != "" {
		t.Error("OpenRouter API key should be empty by default")
	}
}

// TestDefaultConfig_Channels verifies channels are disabled by default
func TestDefaultConfig_Channels(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Channels.Telegram.Enabled {
		t.Error("Telegram should be disabled by default")
	}
	if cfg.Channels.Discord.Enabled {
		t.Error("Discord should be disabled by default")
	}
	if cfg.Channels.Slack.Enabled {
		t.Error("Slack should be disabled by default")
	}
}

// TestDefaultConfig_WebTools verifies web tools config
func TestDefaultConfig_WebTools(t *testing.T) {
	cfg := DefaultConfig()

	// Verify web tools defaults
	if cfg.Tools.Web.Brave.MaxResults != 5 {
		t.Error("Expected Brave MaxResults 5, got ", cfg.Tools.Web.Brave.MaxResults)
	}
	if cfg.Tools.Web.Brave.APIKey != "" {
		t.Error("Brave API key should be empty by default")
	}
	if cfg.Tools.Web.DuckDuckGo.MaxResults != 5 {
		t.Error("Expected DuckDuckGo MaxResults 5, got ", cfg.Tools.Web.DuckDuckGo.MaxResults)
	}
}

func TestSaveConfig_FilePermissions(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("file permission bits are not enforced on Windows")
	}

	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")

	cfg := DefaultConfig()
	if err := SaveConfig(path, cfg); err != nil {
		t.Fatalf("SaveConfig failed: %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0o600 {
		t.Errorf("config file has permission %04o, want 0600", perm)
	}
}

func TestSaveConfig_IncludesEmptyLegacyModelField(t *testing.T) {
	tmpDir := t.TempDir()
	path := filepath.Join(tmpDir, "config.json")

	cfg := DefaultConfig()
	if err := SaveConfig(path, cfg); err != nil {
		t.Fatalf("SaveConfig failed: %v", err)
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile failed: %v", err)
	}

	if !strings.Contains(string(data), `"model": ""`) {
		t.Fatalf("saved config should include empty legacy model field, got: %s", string(data))
	}
}

// TestConfig_Complete verifies all config fields are set
func TestConfig_Complete(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Agents.Defaults.Workspace == "" {
		t.Error("Workspace should not be empty")
	}
	if cfg.Agents.Defaults.Model != "" {
		t.Error("Model should be empty")
	}
	if cfg.Agents.Defaults.Temperature != nil {
		t.Error("Temperature should be nil when not provided")
	}
	if cfg.Agents.Defaults.MaxTokens == 0 {
		t.Error("MaxTokens should not be zero")
	}
	if cfg.Agents.Defaults.MaxToolIterations == 0 {
		t.Error("MaxToolIterations should not be zero")
	}
	if cfg.Gateway.Host != "127.0.0.1" {
		t.Error("Gateway host should have default value")
	}
	if cfg.Gateway.Port == 0 {
		t.Error("Gateway port should have default value")
	}
	if !cfg.Heartbeat.Enabled {
		t.Error("Heartbeat should be enabled by default")
	}
}

func TestDefaultConfig_OpenAIWebSearchEnabled(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.Providers.OpenAI.WebSearch {
		t.Fatal("DefaultConfig().Providers.OpenAI.WebSearch should be true")
	}
}

func TestLoadConfig_OpenAIWebSearchDefaultsTrueWhenUnset(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, []byte(`{"providers":{"openai":{"api_base":""}}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}
	if !cfg.Providers.OpenAI.WebSearch {
		t.Fatal("OpenAI codex web search should remain true when unset in config file")
	}
}

func TestLoadConfig_OpenAIWebSearchCanBeDisabled(t *testing.T) {
	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.json")
	if err := os.WriteFile(configPath, []byte(`{"providers":{"openai":{"web_search":false}}}`), 0o600); err != nil {
		t.Fatalf("WriteFile() error: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}
	if cfg.Providers.OpenAI.WebSearch {
		t.Fatal("OpenAI codex web search should be false when disabled in config file")
	}
}

func TestLoadConfig_WebToolsProxy(t *testing.T) {
	tmpDir := t.TempDir()
	configPath := filepath.Join(tmpDir, "config.json")
	configJSON := `{
  "agents": {"defaults":{"workspace":"./workspace","model":"gpt4","max_tokens":8192,"max_tool_iterations":20}},
  "model_list": [{"model_name":"gpt4","model":"openai/gpt-5.2","api_key":"x"}],
  "tools": {"web":{"proxy":"http://127.0.0.1:7890"}}
}`
	if err := os.WriteFile(configPath, []byte(configJSON), 0o600); err != nil {
		t.Fatalf("os.WriteFile() error: %v", err)
	}

	cfg, err := LoadConfig(configPath)
	if err != nil {
		t.Fatalf("LoadConfig() error: %v", err)
	}
	if cfg.Tools.Web.Proxy != "http://127.0.0.1:7890" {
		t.Fatalf("Tools.Web.Proxy = %q, want %q", cfg.Tools.Web.Proxy, "http://127.0.0.1:7890")
	}
}

// TestDefaultConfig_DMScope verifies the default dm_scope value
func TestDefaultConfig_DMScope(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Session.DMScope != "per-channel-peer" {
		t.Errorf("Session.DMScope = %q, want 'per-channel-peer'", cfg.Session.DMScope)
	}
}

func TestDefaultConfig_WorkspacePath_Default(t *testing.T) {
	// Unset to ensure we test the default
	t.Setenv("PICOCLAW_HOME", "")
	// Set a known home for consistent test results
	t.Setenv("HOME", "/tmp/home")

	cfg := DefaultConfig()
	want := filepath.Join("/tmp/home", ".picoclaw", "workspace")

	if cfg.Agents.Defaults.Workspace != want {
		t.Errorf("Default workspace path = %q, want %q", cfg.Agents.Defaults.Workspace, want)
	}
}

func TestDefaultConfig_WorkspacePath_WithPicoclawHome(t *testing.T) {
	t.Setenv("PICOCLAW_HOME", "/custom/picoclaw/home")

	cfg := DefaultConfig()
	want := "/custom/picoclaw/home/workspace"

	if cfg.Agents.Defaults.Workspace != want {
		t.Errorf("Workspace path with PICOCLAW_HOME = %q, want %q", cfg.Agents.Defaults.Workspace, want)
	}
}

// TestGetAgentByID_Exists verifies that GetAgentByID returns the correct agent
func TestGetAgentByID_Exists(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Agents.Agents = map[string]AgentConfig{
		"agent1": {Name: "Test Agent 1"},
		"agent2": {Name: "Test Agent 2"},
	}

	agent := cfg.GetAgentByID("agent1")
	if agent == nil {
		t.Fatal("GetAgentByID should return agent1")
	}
	if agent.ID != "agent1" || agent.Name != "Test Agent 1" {
		t.Errorf("GetAgentByID returned wrong agent: %+v", agent)
	}
}

// TestGetAgentByID_NotFound verifies that GetAgentByID returns nil for non-existent agent
func TestGetAgentByID_NotFound(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Agents.Agents = map[string]AgentConfig{
		"agent1": {Name: "Test Agent 1"},
	}

	agent := cfg.GetAgentByID("nonexistent")
	if agent != nil {
		t.Errorf("GetAgentByID should return nil for non-existent agent, got: %+v", agent)
	}
}

// TestGetMergedAgentDefaults_MergesWorkspace verifies workspace override
func TestGetMergedAgentDefaults_MergesWorkspace(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Agents.Defaults.Workspace = "/default/workspace"
	cfg.Agents.Agents = map[string]AgentConfig{
		"custom": {Workspace: "/custom/workspace"},
	}

	merged, err := cfg.GetMergedAgentDefaults("custom")
	if err != nil {
		t.Fatalf("GetMergedAgentDefaults error: %v", err)
	}
	if merged.Workspace != "/custom/workspace" {
		t.Errorf("Workspace = %q, want %q", merged.Workspace, "/custom/workspace")
	}
}

// TestGetMergedAgentDefaults_MergesModel verifies model override
func TestGetMergedAgentDefaults_MergesModel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Agents.Defaults.ModelName = "gpt-4"
	cfg.Agents.Agents = map[string]AgentConfig{
		"claude": {Model: &AgentModelConfig{Primary: "claude-3"}},
	}

	merged, err := cfg.GetMergedAgentDefaults("claude")
	if err != nil {
		t.Fatalf("GetMergedAgentDefaults error: %v", err)
	}
	if merged.ModelName != "claude-3" {
		t.Errorf("ModelName = %q, want %q", merged.ModelName, "claude-3")
	}
}

// TestGetMergedAgentDefaults_MergesFallbacks verifies fallbacks override
func TestGetMergedAgentDefaults_MergesFallbacks(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Agents.Defaults.ModelFallbacks = []string{"fallback1"}
	cfg.Agents.Agents = map[string]AgentConfig{
		"multi": {Model: &AgentModelConfig{Primary: "model-1", Fallbacks: []string{"fb1", "fb2"}}},
	}

	merged, err := cfg.GetMergedAgentDefaults("multi")
	if err != nil {
		t.Fatalf("GetMergedAgentDefaults error: %v", err)
	}
	if len(merged.ModelFallbacks) != 2 || merged.ModelFallbacks[0] != "fb1" {
		t.Errorf("ModelFallbacks = %v, want [fb1 fb2]", merged.ModelFallbacks)
	}
}

// TestGetMergedAgentDefaults_KeepsDefaultsForEmptyAgent verifies defaults are kept when agent has no overrides
func TestGetMergedAgentDefaults_KeepsDefaultsForEmptyAgent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Agents.Defaults.Workspace = "/default"
	cfg.Agents.Defaults.ModelName = "gpt-4"
	cfg.Agents.Defaults.MaxTokens = 8192
	cfg.Agents.Agents = map[string]AgentConfig{
		"empty": {},
	}

	merged, err := cfg.GetMergedAgentDefaults("empty")
	if err != nil {
		t.Fatalf("GetMergedAgentDefaults error: %v", err)
	}
	if merged.Workspace != "/default" {
		t.Errorf("Workspace = %q, want %q", merged.Workspace, "/default")
	}
	if merged.ModelName != "gpt-4" {
		t.Errorf("ModelName = %q, want %q", merged.ModelName, "gpt-4")
	}
	if merged.MaxTokens != 8192 {
		t.Errorf("MaxTokens = %d, want %d", merged.MaxTokens, 8192)
	}
}

// TestGetMergedAgentDefaults_NotFound verifies error for non-existent agent
func TestGetMergedAgentDefaults_NotFound(t *testing.T) {
	cfg := DefaultConfig()

	_, err := cfg.GetMergedAgentDefaults("nonexistent")
	if err == nil {
		t.Fatal("GetMergedAgentDefaults should return error for non-existent agent")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Errorf("Error message should contain 'not found', got: %v", err)
	}
}

// TestWorkspacePathForAgent verifies workspace path resolution for agents
func TestWorkspacePathForAgent(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Agents.Defaults.Workspace = "/default/workspace"
	cfg.Agents.Agents = map[string]AgentConfig{
		"custom": {Workspace: "~/custom"},
	}

	path, err := cfg.WorkspacePathForAgent("custom")
	if err != nil {
		t.Fatalf("WorkspacePathForAgent error: %v", err)
	}
	// Should expand ~ to home directory
	if !strings.HasSuffix(path, "/custom") {
		t.Errorf("WorkspacePathForAgent = %q, should end with '/custom'", path)
	}
}

// TestWorkspacePathForAgent_NotFound verifies error for non-existent agent
func TestWorkspacePathForAgent_NotFound(t *testing.T) {
	cfg := DefaultConfig()

	_, err := cfg.WorkspacePathForAgent("nonexistent")
	if err == nil {
		t.Fatal("WorkspacePathForAgent should return error for non-existent agent")
	}
}

// TestGetMergedAgentDefaults_PartialOverrides verifies partial merging when agent has only workspace override
func TestGetMergedAgentDefaults_PartialOverrides(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Agents.Defaults.Workspace = "/default/workspace"
	cfg.Agents.Defaults.ModelName = "gpt-4"
	cfg.Agents.Defaults.MaxTokens = 8192
	cfg.Agents.Agents = map[string]AgentConfig{
		"partial": {Workspace: "/custom/workspace"},
	}

	merged, err := cfg.GetMergedAgentDefaults("partial")
	if err != nil {
		t.Fatalf("GetMergedAgentDefaults error: %v", err)
	}
	// Should override workspace
	if merged.Workspace != "/custom/workspace" {
		t.Errorf("Workspace = %q, want %q", merged.Workspace, "/custom/workspace")
	}
	// Should keep defaults for other fields
	if merged.ModelName != "gpt-4" {
		t.Errorf("ModelName = %q, want %q", merged.ModelName, "gpt-4")
	}
	if merged.MaxTokens != 8192 {
		t.Errorf("MaxTokens = %d, want %d", merged.MaxTokens, 8192)
	}
}
