#!/usr/bin/env bash
set -euo pipefail

project_dir="$(cd "$(dirname "$0")/.." && pwd)"
plist="$project_dir/scripts/com.entropy.arb.plist"
user_home="$(/usr/bin/dscl . -read "/Users/$(/usr/bin/id -un)" NFSHomeDirectory | /usr/bin/awk '{print $2}')"
target="$user_home/Library/LaunchAgents/com.entropy.arb.plist"
mkdir -p "$user_home/Library/LaunchAgents"
cp "$plist" "$target"
launchctl bootout "gui/$(id -u)/com.entropy.arb" 2>/dev/null || true
launchctl bootstrap "gui/$(id -u)" "$target"
launchctl enable "gui/$(id -u)/com.entropy.arb"
launchctl kickstart -k "gui/$(id -u)/com.entropy.arb"
echo "launched com.entropy.arb; inspect with: launchctl print gui/$(id -u)/com.entropy.arb"
