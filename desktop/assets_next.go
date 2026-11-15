//go:build nextui

package main

import "embed"

//go:embed all:frontend/dist-next
var frontendAssets embed.FS

const frontendRoot = "frontend/dist-next"
