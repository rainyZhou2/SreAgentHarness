#!/usr/bin/env bash
# Packages the desktop build (run build.sh first) into a release artifact in
# desktop/dist:
#   macOS   -> SRE-Harness-<version>-macos-<arch>.dmg  (+ .zip)
#   Linux   -> sre-harness-desktop-<version>-linux-<arch>.tar.gz
#   Windows -> sre-harness-desktop-<version>-windows-<arch>.zip
# The macOS app is ad-hoc signed only: it is not notarized, so Gatekeeper asks
# the user to confirm the first launch (right-click > Open).
set -euo pipefail
cd "$(dirname "$0")"
version="${DESKTOP_VERSION:-$(sed -n 's/^var Version = "\(.*\)"/\1/p' settings.go)}"
version="${version#v}"
arch="$(uname -m)"; case "$arch" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; esac
out=dist
mkdir -p "$out"
case "$(uname -s)" in
Darwin)
  app="build/bin/SRE Harness.app"
  [ -d "$app" ] || { echo "run ./build.sh first"; exit 1; }
  stage="$(mktemp -d)"
  cp -R "$app" "$stage/"
  ln -s /Applications "$stage/Applications"
  name="SRE-Harness-${version}-macos-${arch}"
  hdiutil create -quiet -volname "SRE Harness ${version}" -srcfolder "$stage" -ov -format UDZO "$out/${name}.dmg"
  ditto -c -k --sequesterRsrc --keepParent "$app" "$out/${name}.zip"
  rm -rf "$stage"
  echo "packaged: $out/${name}.dmg $out/${name}.zip"
  ;;
Linux)
  [ -f build/bin/sre-harness-desktop ] || { echo "run ./build.sh first"; exit 1; }
  name="sre-harness-desktop-${version}-linux-${arch}"
  stage="$(mktemp -d)/${name}"
  mkdir -p "$stage"
  cp build/bin/sre-harness-desktop "$stage/"
  cp -R build/bin/resources "$stage/"
  tar -C "$(dirname "$stage")" -czf "$out/${name}.tar.gz" "$name"
  echo "packaged: $out/${name}.tar.gz"
  ;;
*)
  [ -f build/bin/sre-harness-desktop.exe ] || { echo "run ./build.sh first"; exit 1; }
  name="sre-harness-desktop-${version}-windows-${arch}"
  stage="$(mktemp -d)/${name}"
  mkdir -p "$stage"
  cp build/bin/sre-harness-desktop.exe "$stage/"
  cp -R build/bin/resources "$stage/"
  (cd "$(dirname "$stage")" && 7z a -tzip -bso0 "$OLDPWD/$out/${name}.zip" "$name" || powershell -NoProfile -Command "Compress-Archive -Path '$name' -DestinationPath '$OLDPWD/$out/${name}.zip'")
  echo "packaged: $out/${name}.zip"
  ;;
esac
