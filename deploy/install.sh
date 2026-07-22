#!/usr/bin/env bash
#
# Install Auger as a systemd service.
#
#   cargo build --release --locked     # as your normal user
#   sudo ./deploy/install.sh
#
# Building is left to the caller rather than done here, because cargo lives in
# the invoking user's home and this script runs as root. Running cargo under
# sudo would build into a root-owned target/ and poison the next plain build.
#
# Re-running is safe: the binary and unit are replaced, but a config or env
# file that already exists is never overwritten.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BINARY="${AUGER_BINARY:-$REPO_ROOT/target/release/auger}"

CONF_DIR=/etc/auger
UNIT=/etc/systemd/system/auger.service
INSTALLED_BIN=/usr/local/bin/auger

die() { printf '\nerror: %s\n' "$*" >&2; exit 1; }
note() { printf '  %s\n' "$*"; }

[[ $EUID -eq 0 ]] || die "run with sudo: sudo $0"

# --- The binary ------------------------------------------------------------

[[ -f $BINARY ]] || die "no binary at $BINARY

       Build it first, as your normal user and not under sudo:
           cd $REPO_ROOT && cargo build --release --locked

       Or point this script at one built elsewhere:
           sudo AUGER_BINARY=/path/to/auger $0"

# A binary built in a container or on another machine can link against a newer
# glibc than this host provides, and the failure at `systemctl start` is a bare
# exit code with nothing in the journal to explain it. Find out now instead.
probe="$(mktemp)"
trap 'rm -f "$probe"' EXIT
if ! "$BINARY" --help >/dev/null 2>"$probe"; then
    printf '\nerror: %s does not run on this host:\n\n' "$BINARY" >&2
    sed 's/^/       /' "$probe" >&2
    printf '\n       A "GLIBC_x.yz not found" above means the binary was built against\n'  >&2
    printf '       a newer glibc than this host has. Build it on this machine.\n\n' >&2
    exit 1
fi

echo "installing:"

# --- Service account -------------------------------------------------------

if id auger &>/dev/null; then
    note "user auger already exists"
else
    # --user-group is explicit rather than relying on USERGROUPS_ENAB, because
    # the config file below is installed group-readable to exactly this group.
    useradd --system --user-group --no-create-home \
            --home-dir /var/lib/auger --shell /usr/sbin/nologin auger
    note "created system user auger"
fi

# --- Files -----------------------------------------------------------------

install -d -m 0755 -o root -g root "$CONF_DIR"

install -m 0755 -o root -g root "$BINARY" "$INSTALLED_BIN"
note "$INSTALLED_BIN"

# 0640 root:auger — carries cleartext SQL passwords, so not world-readable, and
# the service reads it after dropping privileges, so the group must be auger.
if [[ -e $CONF_DIR/auger.toml ]]; then
    note "$CONF_DIR/auger.toml already exists, left untouched"
else
    install -m 0640 -o root -g auger \
            "$REPO_ROOT/deploy/auger.systemd.toml" "$CONF_DIR/auger.toml"
    note "$CONF_DIR/auger.toml  (edit: databases, [server.users] password)"
fi

# 0600 root:root — holds the MongoDB URI. systemd reads EnvironmentFile as
# PID 1, before dropping to the auger user, so the service never needs it.
if [[ -e $CONF_DIR/auger.env ]]; then
    note "$CONF_DIR/auger.env already exists, left untouched"
else
    install -m 0600 -o root -g root \
            "$REPO_ROOT/deploy/auger.env.example" "$CONF_DIR/auger.env"
    note "$CONF_DIR/auger.env  (edit: AUGER_MONGO_URI, AUGER_LISTEN)"
fi

install -m 0644 -o root -g root "$REPO_ROOT/deploy/auger.service" "$UNIT"
note "$UNIT"

systemctl daemon-reload
systemctl enable auger.service >/dev/null
note "enabled at boot"

# --- What is left to do ----------------------------------------------------

# `|| true` because grep exits 1 when it finds nothing, which is the good case
# and must not trip set -e.
placeholders="$(grep -l CHANGEME "$CONF_DIR/auger.toml" "$CONF_DIR/auger.env" 2>/dev/null || true)"

echo
if [[ -n $placeholders ]]; then
    echo "installed but NOT started — these still contain CHANGEME:"
    sed 's/^/  /' <<<"$placeholders"
else
    echo "installed."
fi

cat <<EOF

next:
  1. sudo nano $CONF_DIR/auger.env     # AUGER_MONGO_URI, AUGER_LISTEN
     sudo nano $CONF_DIR/auger.toml    # databases, [server.users] password

  2. Prove it reaches MongoDB and sees the collections before involving the
     unit. systemd-run applies the same EnvironmentFile parsing and the same
     unprivileged user the service will use, so a success here means the only
     thing left untested is the listener:

       sudo systemd-run --pty --uid=auger --gid=auger \\
            -p EnvironmentFile=$CONF_DIR/auger.env \\
            $INSTALLED_BIN --config $CONF_DIR/auger.toml --describe

  3. sudo systemctl start auger
     systemctl status auger
     journalctl -u auger -f

  4. If AUGER_LISTEN is not on 127.0.0.1, restrict the port to the hosts that
     need it. Auger has no TLS and its md5 auth is the deprecated PostgreSQL
     scheme, so a firewall rule is doing real work here:

       sudo ufw allow from <superset-host-ip> to any port 5433 proto tcp
EOF
