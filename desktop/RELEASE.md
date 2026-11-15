# Desktop release

The desktop artifacts are built by `build.sh` and `package.sh`. Unsigned builds
remain usable for development, but macOS Gatekeeper and Windows SmartScreen
will warn on first launch.

## Required secrets

| Variable | Used by | Meaning |
|---|---|---|
| `MACOS_SIGN_IDENTITY` | `build.sh` / `package.sh` | Developer ID Application identity, e.g. `Developer ID Application: Acme (TEAMID)` |
| `APPLE_ID` | `package.sh` | Apple ID for notarization |
| `APPLE_TEAM_ID` | `package.sh` | Apple Developer team ID |
| `APPLE_APP_PASSWORD` | `package.sh` | App-specific password for notarization |
| `WINDOWS_CERT_BASE64` | CI | Base64-encoded PFX code-signing certificate |
| `WINDOWS_CERT_FILE` | `package.sh` | Local path to the PFX code-signing certificate |
| `WINDOWS_CERT_PASSWORD` | `package.sh` | PFX password |
| `WINDOWS_TIMESTAMP_URL` | `package.sh` | Optional RFC 3161 timestamp URL |
| `LINUX_GPG_KEY` | `package.sh` | Optional GPG key id for detached signatures |

## Local verification

```bash
cd desktop
./build.sh
./package.sh
codesign --verify --deep --strict "build/bin/SRE Harness.app"   # macOS
spctl --assess --type execute "build/bin/SRE Harness.app"       # notarized macOS
```

`package.sh` writes `dist/SHA256SUMS.txt`; the release workflow attaches the
artifacts, checksums and these signatures.

## CI

Set the secrets above in the repository. The release workflow passes them to
the desktop build/package job. Without them the job produces the same unsigned
artifacts as before.
