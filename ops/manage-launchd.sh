#!/bin/zsh

set -euo pipefail

ROOT="${0:A:h:h}"
LAUNCH_AGENTS_DIR="$HOME/Library/LaunchAgents"
USER_DOMAIN="gui/$(id -u)"

HOURLY_LABEL="dev.granola.local-archive.hourly"
DAILY_LABEL="dev.granola.local-archive.daily"

HOURLY_TARGET="$LAUNCH_AGENTS_DIR/$HOURLY_LABEL.plist"
DAILY_TARGET="$LAUNCH_AGENTS_DIR/$DAILY_LABEL.plist"

HOURLY_TEMPLATE="$ROOT/ops/launchd/granola-local-archive-hourly.plist"
DAILY_TEMPLATE="$ROOT/ops/launchd/granola-local-archive-daily.plist"


usage() {
  cat <<'EOF'
Usage:
  zsh ./ops/manage-launchd.sh install
  zsh ./ops/manage-launchd.sh uninstall
  zsh ./ops/manage-launchd.sh status
  zsh ./ops/manage-launchd.sh run-now hourly
  zsh ./ops/manage-launchd.sh run-now daily
  zsh ./ops/manage-launchd.sh clear-logs
EOF
}


bootout_if_loaded() {
  local target="$1"
  launchctl bootout "$USER_DOMAIN" "$target" >/dev/null 2>&1 || true
}


render_template() {
  local template="$1"
  local target="$2"
  local label="$3"
  local content

  content="$(<"$template")"
  content="${content//__ROOT__/$ROOT}"
  content="${content//__LABEL__/$label}"
  printf '%s\n' "$content" > "$target"
}


bootstrap_label() {
  local target="$1"
  local label="$2"
  bootout_if_loaded "$target"
  launchctl bootstrap "$USER_DOMAIN" "$target"
  launchctl enable "$USER_DOMAIN/$label"
}


copy_templates() {
  mkdir -p "$LAUNCH_AGENTS_DIR"
  render_template "$HOURLY_TEMPLATE" "$HOURLY_TARGET" "$HOURLY_LABEL"
  render_template "$DAILY_TEMPLATE" "$DAILY_TARGET" "$DAILY_LABEL"
}


remove_legacy_agents() {
  local candidate
  for candidate in "$LAUNCH_AGENTS_DIR"/*granola-hourly.plist "$LAUNCH_AGENTS_DIR"/*granola-daily.plist; do
    if [[ ! -e "$candidate" ]]; then
      continue
    fi
    if [[ "$candidate" == "$HOURLY_TARGET" || "$candidate" == "$DAILY_TARGET" ]]; then
      continue
    fi
    bootout_if_loaded "$candidate"
    rm -f "$candidate"
  done
}


install_agents() {
  remove_legacy_agents
  copy_templates
  bootstrap_label "$HOURLY_TARGET" "$HOURLY_LABEL"
  bootstrap_label "$DAILY_TARGET" "$DAILY_LABEL"
  echo "Installed:"
  echo "  $HOURLY_TARGET"
  echo "  $DAILY_TARGET"
}


uninstall_agents() {
  bootout_if_loaded "$HOURLY_TARGET"
  bootout_if_loaded "$DAILY_TARGET"
  remove_legacy_agents
  rm -f "$HOURLY_TARGET" "$DAILY_TARGET"
  echo "Removed launch agents."
}


print_status_for_label() {
  local label="$1"
  local target="$2"
  if [[ -f "$target" ]]; then
    echo "$label target: $target"
  else
    echo "$label target: missing"
  fi

  if launchctl print "$USER_DOMAIN/$label" >/tmp/"$label".status 2>/dev/null; then
    sed -n '1,80p' /tmp/"$label".status
    rm -f /tmp/"$label".status
  else
    echo "$label: not loaded"
  fi
}


status_agents() {
  print_status_for_label "$HOURLY_LABEL" "$HOURLY_TARGET"
  echo
  print_status_for_label "$DAILY_LABEL" "$DAILY_TARGET"
}


run_now() {
  local schedule="${1:-}"
  case "$schedule" in
    hourly)
      launchctl kickstart -k "$USER_DOMAIN/$HOURLY_LABEL"
      ;;
    daily)
      launchctl kickstart -k "$USER_DOMAIN/$DAILY_LABEL"
      ;;
    *)
      usage
      exit 2
      ;;
  esac
}


clear_logs() {
  mkdir -p "$ROOT/archive/logs"
  : > "$ROOT/archive/logs/daily.err.log"
  : > "$ROOT/archive/logs/daily.log"
  : > "$ROOT/archive/logs/hourly.err.log"
  : > "$ROOT/archive/logs/hourly.log"
  echo "Cleared archive logs."
}


command="${1:-}"

case "$command" in
  install)
    install_agents
    ;;
  uninstall)
    uninstall_agents
    ;;
  status)
    status_agents
    ;;
  run-now)
    run_now "${2:-}"
    ;;
  clear-logs)
    clear_logs
    ;;
  *)
    usage
    exit 2
    ;;
esac
