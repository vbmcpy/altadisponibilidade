# ═══════════════════════════════════════════════════════════
# COMANDOS OPERACIONAIS — pg-failover.sh
# ═══════════════════════════════════════════════════════════

# ── FORÇAR FAILBACK IMEDIATO ─────────────────────────────
truncate -s0 /var/lib/pgsql/.failback_error
truncate -s0 /var/lib/pgsql/.failback_done
truncate -s0 /var/lib/pgsql/.failover_done
sed -i 's/^FAILBACK_NOW="${FAILBACK_NOW:-0}"/FAILBACK_NOW="1"/' /usr/local/bin/pg-failover.sh
systemctl restart pg-failover
tail -f /var/log/failover.log

# Após failback concluir — reverter
sed -i 's/^FAILBACK_NOW="1"/FAILBACK_NOW="${FAILBACK_NOW:-0}"/' /usr/local/bin/pg-failover.sh
systemctl restart pg-failover

# ── DESTRAVAR FAILBACK COM ERRO ──────────────────────────
truncate -s0 /var/lib/pgsql/.failback_error
systemctl restart pg-failover

# ── DESTRAVAR CICLO COMPLETO ─────────────────────────────
truncate -s0 /var/lib/pgsql/.failback_error
truncate -s0 /var/lib/pgsql/.failback_done
truncate -s0 /var/lib/pgsql/.failover_done
systemctl restart pg-failover

# ── FAILOVER MANUAL (promover B agora) ───────────────────
# Usar apenas se A estiver confirmadamente offline
runuser -u postgres -- /usr/pgsql-16/bin/pg_ctl \
  -D /var/lib/pgsql/16/data promote
echo "$(date '+%Y-%m-%d %H:%M:%S')" > /var/lib/pgsql/.failover_done
systemctl restart pg-failover

# ── VER STATUS ATUAL ─────────────────────────────────────
# Estado de B
runuser -u postgres -- /usr/pgsql-16/bin/psql -d postgres \
  -tAc "SELECT pg_is_in_recovery();"
# f = master | t = replica

# Flags de controle
echo "failover : $(cat /var/lib/pgsql/.failover_done)"
echo "failback : $(cat /var/lib/pgsql/.failback_done)"
echo "erro     : $(cat /var/lib/pgsql/.failback_error)"

# Log em tempo real
tail -f /var/log/failover.log