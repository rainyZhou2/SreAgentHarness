// Command sre-harness-desktop is the desktop client for the SRE harness:
// a native window (Wails, system WebView) over the harness /v1 API.
package main

import (
	"embed"
	"log"
	"os"

	"github.com/wailsapp/wails/v2"
	"github.com/wailsapp/wails/v2/pkg/options"
	"github.com/wailsapp/wails/v2/pkg/options/assetserver"
	"github.com/wailsapp/wails/v2/pkg/options/mac"
)

//go:embed all:frontend/dist
var assets embed.FS

func main() {
	app := NewApp()
	if addr := os.Getenv("SRE_DESKTOP_PREVIEW"); addr != "" {
		log.Fatal(servePreview(addr, app))
	}
	err := wails.Run(&options.App{
		Title:            "SRE Harness",
		Width:            1320,
		Height:           860,
		MinWidth:         960,
		MinHeight:        600,
		AssetServer:      &assetserver.Options{Assets: assets},
		BackgroundColour: &options.RGBA{R: 247, G: 247, B: 245, A: 255},
		OnStartup:        app.startup,
		OnShutdown:       app.shutdown,
		Bind:             []interface{}{app},
		Mac: &mac.Options{
			About: &mac.AboutInfo{Title: "SRE Harness", Message: "Desktop client for the SRE agent harness"},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
}
