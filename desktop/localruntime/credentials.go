package localruntime

import "github.com/zalando/go-keyring"

const secretService = "SRE Harness"

// SecretStore keeps model API keys out of settings.json. The default
// implementation uses the OS keychain; tests use an in-memory fake.
type SecretStore interface {
	Set(service, user, secret string) error
	Get(service, user string) (string, error)
	Delete(service, user string) error
}

type keychain struct{}

func (keychain) Set(service, user, secret string) error   { return keyring.Set(service, user, secret) }
func (keychain) Get(service, user string) (string, error) { return keyring.Get(service, user) }
func (keychain) Delete(service, user string) error        { return keyring.Delete(service, user) }

// DefaultSecretStore returns the OS keychain-backed store.
func DefaultSecretStore() SecretStore { return keychain{} }

// SecretUser is the keychain account name for one model provider.
func SecretUser(provider string) string { return "model." + provider }

// ModelEnv maps a desktop model selection to the environment variables the
// harness core reads at startup.
func ModelEnv(provider, baseURL, model, key string) map[string]string {
	switch provider {
	case "anthropic":
		return map[string]string{"ANTHROPIC_API_KEY": key, "HARNESS_ANTHROPIC_BASE_URL": baseURL}
	case "openai":
		return map[string]string{"HARNESS_OPENAI_BASE_URL": baseURL, "HARNESS_OPENAI_MODEL": model,
			"HARNESS_OPENAI_API_KEY": key}
	case "gemini":
		return map[string]string{"GEMINI_API_KEY": key, "HARNESS_GEMINI_BASE_URL": baseURL,
			"HARNESS_GEMINI_MODEL": model}
	default:
		return nil
	}
}

// SetModelKey stores a provider key in the OS keychain.
func SetModelKey(store SecretStore, provider, key string) error {
	if key == "" {
		return store.Delete(secretService, SecretUser(provider))
	}
	return store.Set(secretService, SecretUser(provider), key)
}

// ModelKey reads a provider key from the OS keychain.
func ModelKey(store SecretStore, provider string) (string, error) {
	return store.Get(secretService, SecretUser(provider))
}
