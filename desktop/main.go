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
	"github.com/wailsapp/wails/v2/pkg/options/linux"
	"github.com/wailsapp/wails/v2/pkg/options/mac"
)

//go:embed all:frontend/dist
var assets embed.FS

// appIcon is the application icon (About panel, Linux window). The macOS
// bundle icon is built from the same file by build.sh.
//
//go:embed assets/appicon.png
var appIcon []byte

func main() {
	app := NewApp()
	if addr := os.Getenv("SRE_DESKTOP_PREVIEW"); addr != "" {
		log.Fatal(servePreview(addr, app))
	}
	err := wails.Run(&options.App{
		Title:       "SRE Harness",
		Width:       1320,
		Height:      860,
		MinWidth:    960,
		MinHeight:   600,
		AssetServer: &assetserver.Options{Assets: assets},
		// Transparent so the macOS vibrancy behind the glass sidebar shows through.
		BackgroundColour: &options.RGBA{R: 250, G: 250, B: 250, A: 0},
		OnStartup:        app.startup,
		OnShutdown:       app.shutdown,
		Bind:             []interface{}{app},
		Mac: &mac.Options{
			TitleBar:             mac.TitleBarHiddenInset(),
			WindowIsTranslucent:  true,
			WebviewIsTransparent: true,
			About:                &mac.AboutInfo{Title: "SRE Harness", Message: "Desktop client for the SRE agent harness", Icon: appIcon},
		},
		Linux: &linux.Options{Icon: appIcon},
	})
	if err != nil {
		log.Fatal(err)
	}
}
