package status

import (
	"fmt"
	"os"
	"sort"

	"github.com/sipeed/picoclaw/cmd/picoclaw/internal"
	"github.com/sipeed/picoclaw/pkg/auth"
	"github.com/sipeed/picoclaw/pkg/config"
)

func statusCmd(agentID string) {
	cfg, err := internal.LoadConfig()
	if err != nil {
		fmt.Printf("Error loading config: %v\n", err)
		return
	}

	configPath := internal.GetConfigPath()

	fmt.Printf("%s picoclaw Status\n", internal.Logo)
	fmt.Printf("Version: %s\n", internal.FormatVersion())
	build, _ := internal.FormatBuildInfo()
	if build != "" {
		fmt.Printf("Build: %s\n", build)
	}
	fmt.Println()

	if _, err := os.Stat(configPath); err == nil {
		fmt.Println("Config:", configPath, "✓")
	} else {
		fmt.Println("Config:", configPath, "✗")
	}

	workspace := cfg.WorkspacePath()
	if _, err := os.Stat(workspace); err == nil {
		fmt.Println("Workspace:", workspace, "✓")
	} else {
		fmt.Println("Workspace:", workspace, "✗")
	}

	if _, err := os.Stat(configPath); err == nil {
		// Show agents section
		fmt.Println()
		fmt.Println("Agents:")

		// If specific agent requested, show only that one
		if agentID != "" {
			agent := cfg.GetAgentByID(agentID)
			if agent == nil {
				fmt.Printf("  ✗ Agent %q not found\n", agentID)
			} else {
				displayAgent(agent, cfg, true)
			}
		} else {
			// Show default agent
			displayDefaultAgent(&cfg.Agents.Defaults)

			// Show all agents from the map (sorted alphabetically)
			if len(cfg.Agents.Agents) > 0 {
				// Get sorted list of agent IDs
				agentIDs := make([]string, 0, len(cfg.Agents.Agents))
				for id := range cfg.Agents.Agents {
					agentIDs = append(agentIDs, id)
				}
				sort.Strings(agentIDs)

				for _, id := range agentIDs {
					agent := cfg.Agents.Agents[id]
					agent.ID = id // Set the ID from the map key
					displayAgent(&agent, cfg, false)
				}
			}
		}

		fmt.Println()
		fmt.Printf("Model: %s\n", cfg.Agents.Defaults.GetModelName())

		hasOpenRouter := cfg.Providers.OpenRouter.APIKey != ""
		hasAnthropic := cfg.Providers.Anthropic.APIKey != ""
		hasOpenAI := cfg.Providers.OpenAI.APIKey != ""
		hasGemini := cfg.Providers.Gemini.APIKey != ""
		hasZhipu := cfg.Providers.Zhipu.APIKey != ""
		hasQwen := cfg.Providers.Qwen.APIKey != ""
		hasGroq := cfg.Providers.Groq.APIKey != ""
		hasVLLM := cfg.Providers.VLLM.APIBase != ""
		hasMoonshot := cfg.Providers.Moonshot.APIKey != ""
		hasDeepSeek := cfg.Providers.DeepSeek.APIKey != ""
		hasVolcEngine := cfg.Providers.VolcEngine.APIKey != ""
		hasNvidia := cfg.Providers.Nvidia.APIKey != ""
		hasOllama := cfg.Providers.Ollama.APIBase != ""

		status := func(enabled bool) string {
			if enabled {
				return "✓"
			}
			return "not set"
		}
		fmt.Println("OpenRouter API:", status(hasOpenRouter))
		fmt.Println("Anthropic API:", status(hasAnthropic))
		fmt.Println("OpenAI API:", status(hasOpenAI))
		fmt.Println("Gemini API:", status(hasGemini))
		fmt.Println("Zhipu API:", status(hasZhipu))
		fmt.Println("Qwen API:", status(hasQwen))
		fmt.Println("Groq API:", status(hasGroq))
		fmt.Println("Moonshot API:", status(hasMoonshot))
		fmt.Println("DeepSeek API:", status(hasDeepSeek))
		fmt.Println("VolcEngine API:", status(hasVolcEngine))
		fmt.Println("Nvidia API:", status(hasNvidia))
		if hasVLLM {
			fmt.Printf("vLLM/Local: ✓ %s\n", cfg.Providers.VLLM.APIBase)
		} else {
			fmt.Println("vLLM/Local: not set")
		}
		if hasOllama {
			fmt.Printf("Ollama: ✓ %s\n", cfg.Providers.Ollama.APIBase)
		} else {
			fmt.Println("Ollama: not set")
		}

		store, _ := auth.LoadStore()
		if store != nil && len(store.Credentials) > 0 {
			fmt.Println("\nOAuth/Token Auth:")
			for provider, cred := range store.Credentials {
				status := "authenticated"
				if cred.IsExpired() {
					status = "expired"
				} else if cred.NeedsRefresh() {
					status = "needs refresh"
				}
				fmt.Printf("  %s (%s): %s\n", provider, cred.AuthMethod, status)
			}
		}
	}
}

// displayDefaultAgent shows the default agent configuration
func displayDefaultAgent(defaults *config.AgentDefaults) {
	model := defaults.GetModelName()
	if model == "" {
		model = "(not set)"
	}
	fmt.Printf("  %-10s (default)  workspace: %s, model: %s\n",
		"default", defaults.Workspace, model)
}

// displayAgent shows an agent from the agents list, merging with defaults
func displayAgent(agent *config.AgentConfig, cfg *config.Config, isSelected bool) {
	prefix := "  "
	if isSelected {
		prefix = "  → "
	}

	// Use GetMergedAgentDefaults for consistent merging logic
	merged, err := cfg.GetMergedAgentDefaults(agent.ID)
	if err != nil {
		// Fallback to defaults if merge fails
		merged = cfg.Agents.Defaults
	}

	model := merged.GetModelName()
	if model == "" {
		model = "(not set)"
	}

	fmt.Printf("%s%-10s  workspace: %s, model: %s\n",
		prefix, agent.ID, merged.Workspace, model)
}
