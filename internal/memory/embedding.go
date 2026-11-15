package memory

import (
	"encoding/base64"
	"encoding/json"
	"strings"
)

type EmbeddingDescriptor struct {
	Model      string `json:"model"`
	Version    string `json:"version"`
	Dimensions int    `json:"dimensions"`
}

func (d EmbeddingDescriptor) Valid() bool {
	return d.Model != "" && d.Version != "" && d.Dimensions > 0
}

func (d EmbeddingDescriptor) Comparable(other EmbeddingDescriptor) bool {
	return d.Valid() && d == other
}

func (d EmbeddingDescriptor) Key() string {
	if !d.Valid() {
		return ""
	}
	data, _ := json.Marshal(d)
	return base64.RawURLEncoding.EncodeToString(data)
}

func (d EmbeddingDescriptor) Matches(embedding []float32) bool {
	return d.Valid() && len(embedding) == d.Dimensions
}

func LegacyEmbeddingDescriptor(model string, dimensions int) EmbeddingDescriptor {
	if model == "" || dimensions <= 0 {
		return EmbeddingDescriptor{}
	}
	if strings.HasPrefix(model, "hash-uni-bigram-v2-") {
		return EmbeddingDescriptor{Model: "hash-uni-bigram", Version: "v2", Dimensions: dimensions}
	}
	return EmbeddingDescriptor{Model: model, Version: "v1", Dimensions: dimensions}
}
