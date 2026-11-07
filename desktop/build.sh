#!/usr/bin/env bash
# Builds the desktop client without the Wails CLI.
#   ./build.sh          -> build/bin/sre-harness-desktop (Linux/Windows) or
#                          build/bin/SRE Harness.app (macOS)
#   ./build.sh run      -> build, then launch
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"

# bundle_local builds the harness core and, when HARNESS_PG_PREFIX is set,
# copies the PostgreSQL runtime next to the desktop binary. Release builds set
# HARNESS_REQUIRE_LOCAL=1 so a missing PostgreSQL cannot ship silently.
bundle_local() {
  local dest="$1"
  mkdir -p "$dest/bin"
  local core_tmp
  core_tmp="$(mktemp)"
  (cd "$repo_root" && go build -o "$core_tmp" ./cmd/harness)
  mv "$core_tmp" "$dest/bin/harness"
  if [[ -n "${HARNESS_PG_PREFIX:-}" && -d "$HARNESS_PG_PREFIX" ]]; then
    mkdir -p "$dest/lib" "$dest/share"
    cp "$HARNESS_PG_PREFIX/bin/postgres" "$HARNESS_PG_PREFIX/bin/initdb" "$HARNESS_PG_PREFIX/bin/pg_ctl" "$dest/bin/"
    if [[ "$(uname -s)" == "Darwin" ]] && command -v dylibbundler >/dev/null 2>&1; then
      # Homebrew binaries point at /opt/homebrew; rewrite them to the app's
      # Resources/local/lib so the bundle works on a machine without Homebrew.
      for b in postgres initdb pg_ctl; do
        dylibbundler -of -b -x "$dest/bin/$b" -d "$dest/lib" -p '@executable_path/../lib' >/dev/null
      done
      find "$dest/lib" -name '*.dylib' -exec codesign --force --sign - {} \; >/dev/null 2>&1 || true
    else
      cp -R "$HARNESS_PG_PREFIX/lib/." "$dest/lib/"
    fi
    cp -R "$HARNESS_PG_PREFIX/share/." "$dest/share/"
  elif [[ "${HARNESS_REQUIRE_LOCAL:-0}" == "1" ]]; then
    echo "HARNESS_PG_PREFIX is required for a local-capable release" >&2
    exit 1
  else
    echo "warning: HARNESS_PG_PREFIX not set; local runtime will not start" >&2
  fi
}
cd "$(dirname "$0")"
out=build/bin
mkdir -p "$out"
tags="desktop,production"
version="$(sed -n 's/^var Version = "\(.*\)"/\1/p' settings.go)"
case "$(uname -s)" in
Darwin)
  app="$out/SRE Harness.app"
  rm -rf "$app"
  mkdir -p "$app/Contents/MacOS" "$app/Contents/Resources"
  # App icon: assets/appicon.png -> AppIcon.icns (all sizes macOS asks for).
  iconset="$(mktemp -d)/AppIcon.iconset"
  mkdir -p "$iconset"
  for s in 16 32 128 256 512; do
    sips -z $s $s assets/appicon.png --out "$iconset/icon_${s}x${s}.png" >/dev/null
    sips -z $((s * 2)) $((s * 2)) assets/appicon.png --out "$iconset/icon_${s}x${s}@2x.png" >/dev/null
  done
  iconutil -c icns "$iconset" -o "$app/Contents/Resources/AppIcon.icns"
  rm -r "$(dirname "$iconset")"
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
  <key>CFBundleIconFile</key><string>AppIcon</string>
  <key>CFBundlePackageType</key><string>APPL</string>
  <key>CFBundleShortVersionString</key><string>${version}</string>
  <key>CFBundleVersion</key><string>1</string>
  <key>LSMinimumSystemVersion</key><string>11.0</string>
  <key>NSHighResolutionCapable</key><true/>
  <key>NSAppTransportSecurity</key><dict><key>NSAllowsLocalNetworking</key><true/></dict>
</dict></plist>
PLIST
  bundle_local "$app/Contents/Resources/local"
  codesign --force --sign - "$app" >/dev/null 2>&1 || true
  echo "built: $app"
  if [[ "${1:-}" == run ]]; then open "$app"; fi
  ;;
Linux)
  go build -tags "$tags,webkit2_41" -ldflags "-w -s" -o "$out/sre-harness-desktop" .
  bundle_local "$out/resources/local"
  echo "built: $out/sre-harness-desktop"
  if [[ "${1:-}" == run ]]; then "$out/sre-harness-desktop"; fi
  ;;
*)
  go build -tags "$tags" -ldflags "-w -s -H windowsgui" -o "$out/sre-harness-desktop.exe" .
  bundle_local "$out/resources/local"
  echo "built: $out/sre-harness-desktop.exe"
  ;;
esac
