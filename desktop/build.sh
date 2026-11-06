#!/usr/bin/env bash
# Builds the desktop client without the Wails CLI.
#   ./build.sh          -> build/bin/sre-harness-desktop (Linux/Windows) or
#                          build/bin/SRE Harness.app (macOS)
#   ./build.sh run      -> build, then launch
set -euo pipefail
cd "$(dirname "$0")"
out=build/bin
mkdir -p "$out"
tags="desktop,production"
case "$(uname -s)" in
Darwin)
  app="$out/SRE Harness.app"
  rm -rf "$app"
  mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources"
  CGO_ENABLED=1 CGO_LDFLAGS="-framework UniformTypeIdentifiers -mmacosx-version-min=11.0" \
  CGO_CFLAGS="-mmacosx-version-min=11.0" \
    go build -tags "$tags" -ldflags "-w -s" -o "$app/Contents/MacOS/sre-harness-desktop" .
  cat > "$app/Contents/Info.plist" <<PLIST
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0"><dict>
  <key>CFBundleName</key><string>SRE Harness</string>
  <key>CFBundleDisplayName</key><string>SRE Harness</string>
  <key>CFBundleIdentifier</key><string>dev.sreharness.desktop</string>
  <key>CFBundleExecutable</key><string>sre-harness-desktop</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>0.1.0</string>
  <key>CFBundleVersion</key><string>1</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>NSHighResolutionCapable</key><true/>
  <key>NSAppTransportSecurity</key><dict><key>NSAllowsLocalNetworking</key><true/></dict>
</dict></plist>
PLIST
  codesign --force --sign - "$app" >/dev/null 2>&1 || true
  echo "built: $app"
  if [[ "${1:-}" == run ]]; then open "$app"; fi
  ;;
Linux)
  go build -tags "$tags,webkit2_41" -ldflags "-w -s" -o "$out/sre-harness-desktop" .
  echo "built: $out/sre-harness-desktop"
  if [[ "${1:-}" == run ]]; then "$out/sre-harness-desktop"; fi
  ;;
*)
  go build -tags "$tags" -ldflags "-w -s -H windowsgui" -o "$out/sre-harness-desktop.exe" .
  echo "built: $out/sre-harness-desktop.exe"
  ;;
esac
