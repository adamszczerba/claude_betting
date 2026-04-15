#!/bin/bash
set -e
CONF=/etc/wireguard/wg0.conf
CLEAN_CONF=/tmp/wg0-clean.conf
echo "[entrypoint] Configuring WireGuard manually..."
WG_ADDRS=$(grep -iE "^\s*Address\s*=" "$CONF" | sed 's/.*=\s*//' | head -1)
WG_DNS=$(grep -iE "^\s*DNS\s*=" "$CONF" | sed 's/.*=\s*//' | head -1)
grep -ivE "^\s*(Address|DNS)\s*=" "$CONF" > "$CLEAN_CONF"
ip link add dev wg0 type wireguard
wg setconf wg0 "$CLEAN_CONF"
for addr in $(echo "$WG_ADDRS" | tr ',' ' '); do
    addr=$(echo "$addr" | tr -d ' ')
    [ -n "$addr" ] && ip address add "$addr" dev wg0
done
ip link set mtu 1420 up dev wg0
wg set wg0 fwmark 51820
ip -4 route add 0.0.0.0/0 dev wg0 table 51820
ip -4 rule add not fwmark 51820 table 51820
ip -4 rule add table main suppress_prefixlength 0
ip -6 route add ::/0 dev wg0 table 51820 2>/dev/null || true
ip -6 rule add not fwmark 51820 table 51820 2>/dev/null || true
ip -6 rule add table main suppress_prefixlength 0 2>/dev/null || true
if [ -n "$WG_DNS" ]; then
    DNS1=$(echo "$WG_DNS" | cut -d',' -f1 | tr -d ' ')
    echo "nameserver $DNS1" > /etc/resolv.conf
    echo "[entrypoint] DNS set to $DNS1"
fi
echo "[entrypoint] WireGuard tunnel active:"
wg show wg0
echo ""
echo "[entrypoint] Public IP via VPN:"
curl -s --max-time 15 https://ifconfig.me || echo "(could not reach ifconfig.me)"
echo ""
echo "[entrypoint] Starting Pinnacle scraper..."
exec python /app/pinnacle_scraper.py -o /app/db "$@"
