#!/usr/bin/env bash
# Packages the desktop build (run build.sh first) into a release artifact in
# desktop/dist:
#   macOS   -> SRE-Harness-<version>-macos-<arch>.dmg  (+ .zip)
#   Linux   -> sre-harness-desktop-<version>-linux-<arch>.tar.gz
#   Windows -> sre-harness-desktop-<version>-windows-<arch>.zip
# Unsigned builds are ad-hoc signed and not notarized, so Gatekeeper asks the
# user to confirm the first launch. Set the signing/notarization environment
# variables documented in RELEASE.md for a distributable build.
set -euo pipefail
cd "$(dirname "$0")"
version="${DESKTOP_VERSION:-$(sed -n 's/^var Version = "\(.*\)"/\1/p' settings.go)}"
version="${version#v}"
arch="$(uname -m)"; case "$arch" in x86_64) arch=amd64 ;; aarch64) arch=arm64 ;; esac

out=dist
rm -rf "$out"
mkdir -p "$out"
case "$(uname -s)" in
Darwin)
  app="build/bin/SRE Harness.app"
  [ -d "$app" ] || { echo "run ./build.sh first"; exit 1; }
  if [[ -n "${MACOS_SIGN_IDENTITY:-}" ]]; then
    codesign --verify --deep --strict "$app"
  fi
  if [[ -n "${APPLE_ID:-}" || -n "${APPLE_TEAM_ID:-}" || -n "${APPLE_APP_PASSWORD:-}" ]]; then
    : "${APPLE_ID:?APPLE_ID is required for notarization}"
    : "${APPLE_TEAM_ID:?APPLE_TEAM_ID is required for notarization}"
    : "${APPLE_APP_PASSWORD:?APPLE_APP_PASSWORD is required for notarization}"
    notarize_zip="$out/notarize.zip"
    ditto -c -k --sequesterRsrc --keepParent "$app" "$notarize_zip"
    xcrun notarytool submit "$notarize_zip" --apple-id "$APPLE_ID" --team-id "$APPLE_TEAM_ID" --password "$APPLE_APP_PASSWORD" --wait
    rm -f "$notarize_zip"
    xcrun stapler staple "$app"
    spctl --assess --type execute --verbose=4 "$app"
  fi
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
  if [[ -n "${LINUX_GPG_KEY:-}" ]]; then
    gpg --batch --yes --local-user "$LINUX_GPG_KEY" --detach-sign --armor "$out/${name}.tar.gz"
    echo "signed: $out/${name}.tar.gz.asc"
  fi
  echo "packaged: $out/${name}.tar.gz"
  ;;
*)
  [ -f build/bin/sre-harness-desktop.exe ] || { echo "run ./build.sh first"; exit 1; }
  name="sre-harness-desktop-${version}-windows-${arch}"
  stage="$(mktemp -d)/${name}"
  mkdir -p "$stage"
  exe=build/bin/sre-harness-desktop.exe
  if [[ -n "${WINDOWS_CERT_FILE:-}" ]]; then
    command -v signtool >/dev/null 2>&1 || { echo "signtool is required for Windows signing"; exit 1; }
    signtool sign /fd sha256 /td sha256 /tr "${WINDOWS_TIMESTAMP_URL:-http://timestamp.digicert.com}" \
      /f "$WINDOWS_CERT_FILE" /p "${WINDOWS_CERT_PASSWORD:?WINDOWS_CERT_PASSWORD is required}" "$exe"
    signtool verify /pa "$exe"
  fi
  cp "$exe" "$stage/"
  cp -R build/bin/resources "$stage/"
  (cd "$(dirname "$stage")" && 7z a -tzip -bso0 "$OLDPWD/$out/${name}.zip" "$name" || powershell -NoProfile -Command "Compress-Archive -Path '$name' -DestinationPath '$OLDPWD/$out/${name}.zip'")
  echo "packaged: $out/${name}.zip"
  ;;
esac

if command -v shasum >/dev/null 2>&1; then
  (cd "$out" && shasum -a 256 * > SHA256SUMS.txt)
elif command -v sha256sum >/dev/null 2>&1; then
  (cd "$out" && sha256sum * > SHA256SUMS.txt)
fi
