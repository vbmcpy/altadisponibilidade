#!/bin/bash
# pg-failover.sh — Failover + Failback automático PostgreSQL HA (hardened v2)
# Gerenciado por: pg-failover.service (systemd)
#
# REGRAS DE CONEXÃO:
#   LOCAL  (B): runuser -u postgres -- psql -d postgres       (socket Unix, sem senha)
#   REMOTO (A): ssh_a "sudo runuser -u postgres -- psql -d postgres"  (socket Unix, sem senha)
#   B→A ou A→B: usa IP + REPL_USER + pgpass                  (TCP com autenticação)
#
# MELHORIAS v2:
#   [1] Health check triplo: ping + SSH + PostgreSQL
#   [2] Verificação de lag WAL antes do failover (evita perda de dados)
#   [3] Verificação de lag=0 antes do failback
#   [4] Credenciais carregadas de arquivo .env (segurança)
#   [5] Failback manual recomendado em produção (FAILBACK_NOW=1)
#
# MELHORIAS v3 (hardened):
#   [6] Health check duplo: LAN (DRG) + rede pública
#       - LAN FAIL + pública OK  → DRG caiu mas A está vivo → alerta, SEM failover
#       - LAN FAIL + pública FAIL → A confirmado offline → failover normal
#       - Evita failover indevido por queda isolada do túnel entre sites
#
# CORREÇÕES v3.1:
#   [7] Removidos "local" inválidos do main loop (bug silencioso com set -uo)
#   [8] get_master_hostname movido para branch "A ONLINE" (evita timeout desnecessário)
#   [9] check_a_via_public com short-circuit (reduz até 33s extras no RTO em worst case)


usermod -aG totalip postgres

set -uo pipefail

########################################
# CARREGAR CREDENCIAIS DE ARQUIVO .ENV
########################################
ENV_FILE="/etc/pg-failover.env"
if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$ENV_FILE"
else
    REPL_PASSWORD="${REPL_PASSWORD:-totalipHa}"
    TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-8530438377:AAFRs73B25PMMHKeT-5PbjAj-IF6ZzrjBas}"
    TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-1144847708}"
fi

########################################
# CONFIG
########################################
# IPs privados (LAN — roteados pelo DRG)
MASTER_A_IP="10.0.0.167"
SLAVE_B_IP="192.168.0.239"

# IPs públicos (usados como segunda camada de verificação)
MASTER_A_PUBLIC_IP="146.235.50.29"
SLAVE_B_PUBLIC_IP="204.216.167.122"

# ── Versão do PostgreSQL — altere APENAS esta linha ──────────────────────
PG_VER="13"
PGDATA="/var/lib/pgsql/${PG_VER}/data"
PG_BIN="/usr/pgsql-${PG_VER}/bin"
PG_SERVICE="postgresql-${PG_VER}"

PG_SERVICE="postgresql-${PG_VER}"

PG_SOCKET_DIR="${PG_SOCKET_DIR:-}"

REPL_USER="replytotalip"
DB_NAME="totalipdb"
HEALTH_DB="postgres"

CHECK_SYSTEM_CMD="/usr/local/rbenv/shims/ruby /home/totalip/ipserver/nagios/check_system.rb -v"

MY_SLOT="replica2"
REMOTE_SLOT="replica1"

LOGDIR="/var/log"
LOGFILE="$LOGDIR/failover.log"

FLAGDIR="/var/lib/pgsql"
FLAG_FAILOVER="$FLAGDIR/.failover_done"
FLAG_FAILBACK="$FLAGDIR/.failback_done"
FAILBACK_ERROR_FLAG="$FLAGDIR/.failback_error"

SSH_USER="rsync"
SSH_KEY="/home/rsync/.ssh/id_rsa"
SSH_OPTS="-i $SSH_KEY -o IdentitiesOnly=yes -o BatchMode=yes \
          -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
          -o ServerAliveInterval=10 -o ServerAliveCountMax=3"

# SSH opts para rede pública — timeout menor pra não travar o loop
SSH_OPTS_PUBLIC="-i $SSH_KEY -o IdentitiesOnly=yes -o BatchMode=yes \
                 -o StrictHostKeyChecking=no -o ConnectTimeout=8 \
                 -o ServerAliveInterval=5 -o ServerAliveCountMax=2"

BACKUP_DB_DIR="/backup/backup_db"

#--------------------------------------
# TUNABLES
#--------------------------------------
FAILOVER_THRESHOLD=3
FAILBACK_HEALTH_REQUIRED=4
CHECK_INTERVAL=5
BASEBACKUP_TIMEOUT=3600
DISK_MIN_FREE_MB=5120
MAX_LAG_FOR_FAILOVER=1048576   # 1MB
MAX_LAG_FOR_FAILBACK=1048576   # 1MB

#--------------------------------------
# FAILBACK AGENDADO
#--------------------------------------
FAILBACK_SCHEDULE_TIME="04:00"
FAILBACK_SCHEDULE_WINDOW_MIN=30
FAILBACK_NOW="${FAILBACK_NOW:-0}"

########################################
# INICIALIZAÇÃO
########################################
mkdir -p "$LOGDIR" "$FLAGDIR"
touch "$LOGFILE" "$FLAG_FAILOVER" "$FLAG_FAILBACK" "$FAILBACK_ERROR_FLAG"
chmod 640 "$LOGFILE"
chmod 600 "$FLAG_FAILOVER" "$FLAG_FAILBACK" "$FAILBACK_ERROR_FLAG"

FAILURE_COUNT=0
MASTER_HEALTH_COUNT=0
SPLITBRAIN_PROTECTED=0
SLAVE_HOSTNAME=$(hostname)
MASTER_HOSTNAME="unknown"

# Contador de alertas DRG para evitar flood de mensagens
DRG_ALERT_COUNT=0
DRG_ALERT_MAX=3   # envia alerta no primeiro, depois a cada N*CHECK_INTERVAL

########################################
# LOG
########################################
timestamp() { date "+%Y-%m-%d %H:%M:%S"; }
log()       { echo "$(timestamp) $1" | tee -a "$LOGFILE"; }
log_warn()  { log "WARNING  $1"; }
log_error() { log "ERROR    $1"; }
log_ok()    { log "OK       $1"; }
log_info()  { log "INFO     $1"; }

send_telegram() {
    local MSG="$1"
    curl -s --max-time 10 -X POST \
        "https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage" \
        -d chat_id="${TELEGRAM_CHAT_ID}" \
        -d text="${MSG}" \
        -d parse_mode="Markdown" > /dev/null 2>&1 \
    || log_warn "Falha ao enviar mensagem Telegram"
}

########################################
# CHECK_SYSTEM
########################################
run_check_system_local() {
    log_info "Encerrando check_system anterior local (se existir)..."
    pkill -f "check_system.rb" 2>/dev/null || true
    pkill -f "ruby.*check_system" 2>/dev/null || true
    sleep 2
    log_info "Executando check_system local (B)..."
    if $CHECK_SYSTEM_CMD >> "$LOGFILE" 2>&1; then
        log_ok "check_system local executado com sucesso."
    else
        log_warn "check_system local retornou erro (nao critico)."
    fi
}

run_check_system_remote_a() {
    log_info "Encerrando check_system anterior em A (se existir)..."
    ssh_a "pkill -f 'check_system.rb' 2>/dev/null; pkill -f 'ruby.*check_system' 2>/dev/null; true" 2>/dev/null || true
    sleep 2
    log_info "Executando check_system em A..."
    if ssh_a "sudo $CHECK_SYSTEM_CMD >> /var/log/failover.log 2>&1"; then
        log_ok "check_system em A executado com sucesso."
    else
        log_warn "check_system em A retornou erro (nao critico)."
    fi
}

########################################
# METRICAS DO SISTEMA
########################################
get_disk_info_local() {
    df -h /var/lib/pgsql 2>/dev/null | awk 'NR==2{printf "%s usado de %s (%s livre)", $3, $2, $4}'
}
get_disk_info_remote() {
    ssh_a "df -h /var/lib/pgsql 2>/dev/null | awk 'NR==2{printf \"%s usado de %s (%s livre)\", \$3, \$2, \$4}'" 2>/dev/null || echo "N/A"
}
get_mem_info_local() {
    free -h 2>/dev/null | awk '/^Mem:/{printf "%s usado de %s", $3, $2}'
}
get_mem_info_remote() {
    ssh_a "free -h 2>/dev/null | awk '/^Mem:/{printf \"%s usado de %s\", \$3, \$2}'" 2>/dev/null || echo "N/A"
}
get_load_local() {
    uptime 2>/dev/null | awk -F'load average:' '{gsub(/ /,"",$2); print $2}' | cut -d, -f1-3
}
get_load_remote() {
    ssh_a "uptime 2>/dev/null | awk -F'load average:' '{print \$NF}'" 2>/dev/null || echo "N/A"
}
get_cpu_info_local() {
    top -bn1 2>/dev/null | grep "Cpu(s)" | awk '{printf "%.1f%%", $2 + $4}' || echo "N/A"
}
get_cpu_info_remote() {
    ssh_a "top -bn1 2>/dev/null | grep 'Cpu(s)' | awk '{printf \"%.1f%%\", \$2 + \$4}'" 2>/dev/null || echo "N/A"
}
get_uptime_local() {
    uptime -p 2>/dev/null || uptime 2>/dev/null | awk -F'up ' '{print $2}' | cut -d',' -f1-2 || echo "N/A"
}
get_uptime_remote() {
    ssh_a "uptime -p 2>/dev/null || uptime 2>/dev/null | awk -F'up ' '{print \$2}' | cut -d',' -f1-2" 2>/dev/null || echo "N/A"
}

send_status_report() {
    local CONTEXT="${1:-heartbeat}"
    local ROLE_A ROLE_B
    local STATE_A STATE_B
    STATE_A=$(get_pg_state_remote_a)
    STATE_B=$(get_pg_state_local)
    case "$STATE_A" in
        master)  ROLE_A="✅ Master"  ;;
        replica) ROLE_A="🔄 Réplica" ;;
        *)       ROLE_A="❌ Offline" ;;
    esac
    case "$STATE_B" in
        master)  ROLE_B="✅ Master"  ;;
        replica) ROLE_B="🔄 Réplica" ;;
        *)       ROLE_B="❌ Offline" ;;
    esac
    local LAG
    LAG=$(get_local_wal_lag_bytes 2>/dev/null || echo "N/A")
    local DISK_A DISK_B MEM_A MEM_B LOAD_A LOAD_B CPU_A CPU_B UP_A UP_B
    DISK_A=$(get_disk_info_remote); DISK_B=$(get_disk_info_local)
    MEM_A=$(get_mem_info_remote);   MEM_B=$(get_mem_info_local)
    LOAD_A=$(get_load_remote);      LOAD_B=$(get_load_local)
    CPU_A=$(get_cpu_info_remote);   CPU_B=$(get_cpu_info_local)
    UP_A=$(get_uptime_remote);      UP_B=$(get_uptime_local)
    send_telegram "📡 *STATUS HA — ${CONTEXT}*

🖥 *Node A:* ${MASTER_HOSTNAME} \`(${MASTER_A_IP} / ${MASTER_A_PUBLIC_IP})\`
   Modo: ${ROLE_A}
   Uptime: ${UP_A}
   💾 Disco: ${DISK_A}
   🧠 Memória: ${MEM_A}
   ⚡ Load: ${LOAD_A} | CPU: ${CPU_A}

🖥 *Node B:* ${SLAVE_HOSTNAME} \`(${SLAVE_B_IP} / ${SLAVE_B_PUBLIC_IP})\`
   Modo: ${ROLE_B}
   Uptime: ${UP_B}
   💾 Disco: ${DISK_B}
   🧠 Memória: ${MEM_B}
   ⚡ Load: ${LOAD_B} | CPU: ${CPU_B}

📡 *Lag WAL:* ${LAG} bytes
🕐 ${CONTEXT} | $(timestamp)"
}

get_pg_version_local() {
    runuser -u postgres -- "$PG_BIN/psql" -d "$HEALTH_DB" \
        -tAc "SELECT version();" 2>/dev/null | awk '{print $1, $2}' || echo "N/A"
}

calc_duration() {
    local START="$1"
    local END; END=$(date +%s)
    local DIFF=$(( END - START ))
    local H=$(( DIFF / 3600 ))
    local M=$(( (DIFF % 3600) / 60 ))
    local S=$(( DIFF % 60 ))
    if [ $H -gt 0 ]; then echo "${H}h ${M}m ${S}s"
    elif [ $M -gt 0 ]; then echo "${M}m ${S}s"
    else echo "${S}s"; fi
}

FAILOVER_START_TS=0
FAILBACK_START_TS=0

########################################
# SSH — LAN (via DRG)
########################################
ssh_a() {
    ssh $SSH_OPTS "${SSH_USER}@${MASTER_A_IP}" "$@"
}

ssh_a_retry() {
    local MAX_RETRIES=3
    local DELAY=5
    local i=0
    while [ $i -lt $MAX_RETRIES ]; do
        if ssh_a "$@"; then return 0; fi
        i=$((i + 1))
        log_warn "ssh_a falhou (tentativa $i/$MAX_RETRIES). Aguardando ${DELAY}s..."
        sleep "$DELAY"
        DELAY=$((DELAY * 2))
    done
    log_error "ssh_a_retry: todas as tentativas falharam"
    return 1
}

########################################
# SSH — Rede pública
########################################
ssh_a_public() {
    ssh $SSH_OPTS_PUBLIC "${SSH_USER}@${MASTER_A_PUBLIC_IP}" "$@"
}

get_master_hostname() {
    MASTER_HOSTNAME=$(ssh_a "hostname" 2>/dev/null || echo "unknown")
    echo "$MASTER_HOSTNAME"
}

########################################
# HEALTH CHECK TRIPLO — LAN (DRG)
########################################
check_ping_a() {
    ping -c 2 -W 2 "$MASTER_A_IP" >/dev/null 2>&1
}

check_ssh_a() {
    ssh $SSH_OPTS -o ConnectTimeout=2 \
        "${SSH_USER}@${MASTER_A_IP}" "echo ok" >/dev/null 2>&1
}

check_pg_a() {
    ssh_a "sudo runuser -u postgres -- $PG_BIN/psql \
        -d $HEALTH_DB \
        -tAc 'SELECT 1' >/dev/null 2>&1" 2>/dev/null
}

########################################
# HEALTH CHECK TRIPLO — Rede pública
#
# Usa a mesma chave SSH e usuário que o check LAN.
# ConnectTimeout maior pra tolerar latência pública.
# check_pg_a_public só é chamado se SSH público responder.
########################################
check_ping_a_public() {
    ping -c 2 -W 3 "$MASTER_A_PUBLIC_IP" >/dev/null 2>&1
}

check_ssh_a_public() {
    ssh $SSH_OPTS_PUBLIC \
        "${SSH_USER}@${MASTER_A_PUBLIC_IP}" "echo ok" >/dev/null 2>&1
}

check_pg_a_public() {
    ssh_a_public "sudo runuser -u postgres -- $PG_BIN/psql \
        -d $HEALTH_DB \
        -tAc 'SELECT 1' >/dev/null 2>&1" 2>/dev/null
}

# Retorna 0 se A responde em pelo menos um check pela rede pública
# Retorna 1 se todos os três falharam pela rede pública
# Short-circuit: para no primeiro check que passar, evitando timeouts desnecessários
# quando A já está confirmado vivo (ou confirmado morto)
check_a_via_public() {
    if check_ping_a_public; then
        log_info "Check público — ping OK (A vivo)"
        return 0
    fi
    if check_ssh_a_public; then
        log_info "Check público — ssh OK (A vivo)"
        return 0
    fi
    if check_pg_a_public; then
        log_info "Check público — pg OK (A vivo)"
        return 0
    fi
    log_info "Check público — ping/ssh/pg falharam (A offline ou internet de B indisponível)"
    return 1
}

check_master_online() {
    check_ping_a && check_ssh_a
}

########################################
# DETECÇÃO DE FALHA COM DISTINÇÃO DRG vs SERVIDOR
#
# Lógica:
#   1. Testa os 3 checks pela LAN (DRG)
#   2. Se LAN OK em qualquer camada → A está vivo, reseta contador
#   3. Se LAN FALHOU nos 3 → testa pela rede pública
#      a. Pública OK → DRG caiu mas A está vivo
#            → alerta Telegram, reseta contador, NÃO faz failover
#      b. Pública FAIL → A genuinamente offline
#            → incrementa contador, failover ao atingir threshold
########################################
master_confirmed_offline() {
    local PING_OK=0 SSH_OK=0 PG_OK=0

    check_ping_a && PING_OK=1
    check_ssh_a  && SSH_OK=1
    [ $SSH_OK -eq 1 ] && check_pg_a && PG_OK=1

    # LAN ainda responde — tudo normal
    if [ $PING_OK -eq 1 ] || [ $SSH_OK -eq 1 ] || [ $PG_OK -eq 1 ]; then
        if [ $FAILURE_COUNT -gt 0 ]; then
            log_info "A respondeu pela LAN (ping=${PING_OK} ssh=${SSH_OK} pg=${PG_OK}). Resetando contador."
        fi
        FAILURE_COUNT=0
        DRG_ALERT_COUNT=0
        return 1
    fi

    # LAN falhou — verificar pela rede pública antes de decidir
    log_warn "LAN de A offline (ping=${PING_OK} ssh=${SSH_OK} pg=${PG_OK}) — verificando pela rede pública..."

    if check_a_via_public; then
        # A está vivo, mas DRG/túnel está fora
        DRG_ALERT_COUNT=$((DRG_ALERT_COUNT + 1))

        # Envia alerta apenas na primeira detecção e depois periodicamente
        # para não gerar flood no Telegram
        if [ "$DRG_ALERT_COUNT" -eq 1 ] || [ $(( DRG_ALERT_COUNT % 12 )) -eq 0 ]; then
            log_warn "DRG OFFLINE — A (${MASTER_A_PUBLIC_IP}) responde pela internet. Sem failover."
            send_telegram "⚠️ *ALERTA — DRG / TÚNEL OFFLINE*

🔌 Conectividade LAN entre os sites foi perdida
✅ *Node A:* ${MASTER_HOSTNAME} (${MASTER_A_PUBLIC_IP}) — respondendo pela internet
🖥 *Node B:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP}) — aguardando

⛔ *Failover NÃO iniciado* — A está operacional
🔧 Verifique o DRG / túnel IPSec na OCI

🕐 Horário: $(timestamp)"
        else
            log_warn "DRG ainda offline (alerta ${DRG_ALERT_COUNT}) — A vivo pela internet. Aguardando recuperação do túnel."
        fi

        FAILURE_COUNT=0
        return 1   # Não failover
    fi

    # Pública também falhou — A está genuinamente offline
    DRG_ALERT_COUNT=0
    FAILURE_COUNT=$((FAILURE_COUNT + 1))
    log_warn "A offline em LAN e rede pública (falha ${FAILURE_COUNT}/${FAILOVER_THRESHOLD})"
    [ "$FAILURE_COUNT" -ge "$FAILOVER_THRESHOLD" ]
}

########################################
# ESTADO POSTGRESQL LOCAL (B)
########################################
get_pg_state_local() {
    local RESULT
    local SOCK_ARGS=""
    [ -n "$PG_SOCKET_DIR" ] && SOCK_ARGS="-h $PG_SOCKET_DIR"
    RESULT=$(runuser -u postgres -- "$PG_BIN/psql" $SOCK_ARGS \
        -d "$HEALTH_DB" \
        -tAc "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d '[:space:]')
    if [ -z "$RESULT" ] && [ -z "$PG_SOCKET_DIR" ]; then
        RESULT=$(runuser -u postgres -- "$PG_BIN/psql" -h /tmp \
            -d "$HEALTH_DB" \
            -tAc "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d '[:space:]')
    fi
    case "$RESULT" in
        f) echo "master"  ;;
        t) echo "replica" ;;
        *) echo "down"    ;;
    esac
}

get_pg_state() {
    local S; S=$(get_pg_state_local)
    case "$S" in
        master)  echo "f"     ;;
        replica) echo "t"     ;;
        *)       echo "error" ;;
    esac
}

check_if_promoted() {
    [ "$(get_pg_state_local)" = "master" ]
}

########################################
# ESTADO POSTGRESQL REMOTO (A)
########################################
get_pg_state_remote_a() {
    local RESULT
    RESULT=$(ssh_a "sudo runuser -u postgres -- $PG_BIN/psql \
        -d $HEALTH_DB \
        -tAc 'SELECT pg_is_in_recovery();'" 2>/dev/null | tr -d '[:space:]')
    case "$RESULT" in
        f) echo "master"  ;;
        t) echo "replica" ;;
        *) echo "down"    ;;
    esac
}

########################################
# VERIFICAÇÃO DE LAG WAL
########################################
get_local_wal_lag_bytes() {
    runuser -u postgres -- "$PG_BIN/psql" ${PG_SOCKET_DIR:+-h $PG_SOCKET_DIR} \
        -d "$HEALTH_DB" \
        -tAc "SELECT COALESCE(
                pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()),
                0
              );" 2>/dev/null | tr -d '[:space:]' || echo "999999999"
}

check_local_lag_acceptable() {
    if [ "$MAX_LAG_FOR_FAILOVER" -eq 0 ]; then
        log_info "Verificacao de lag desabilitada (MAX_LAG_FOR_FAILOVER=0)"
        return 0
    fi
    local LAG; LAG=$(get_local_wal_lag_bytes)
    if ! echo "$LAG" | grep -qE '^[0-9]+$'; then
        log_warn "Nao foi possivel verificar lag. Prosseguindo com failover."
        return 0
    fi
    if [ "$LAG" -le "$MAX_LAG_FOR_FAILOVER" ]; then
        log_ok "Lag de B aceitavel: ${LAG} bytes (<= ${MAX_LAG_FOR_FAILOVER})"
        return 0
    else
        log_warn "Lag de B alto: ${LAG} bytes (> ${MAX_LAG_FOR_FAILOVER}) — dados podem ser perdidos no failover"
        return 1
    fi
}

get_remote_replication_lag() {
    runuser -u postgres -- "$PG_BIN/psql" \
        -d "$HEALTH_DB" \
        -tAc "SELECT COALESCE(
                pg_wal_lsn_diff(pg_current_wal_lsn(),
                (SELECT replay_lsn FROM pg_stat_replication
                 WHERE application_name = 'nodeA' OR client_addr = '${MASTER_A_IP}'
                 LIMIT 1)),
                999999999
              );" 2>/dev/null | tr -d '[:space:]' || echo "999999999"
}

check_remote_lag_for_failback() {
    if [ "$MAX_LAG_FOR_FAILBACK" -eq 0 ]; then
        log_info "Verificacao de lag para failback desabilitada"
        return 0
    fi
    local LAG; LAG=$(get_remote_replication_lag)
    if ! echo "$LAG" | grep -qE '^[0-9]+$'; then
        log_warn "Nao foi possivel verificar lag de A. Abortando failback por seguranca."
        return 1
    fi
    if [ "$LAG" -le "$MAX_LAG_FOR_FAILBACK" ]; then
        log_ok "Lag de A aceitavel para failback: ${LAG} bytes"
        return 0
    else
        log_warn "A ainda com lag alto para failback: ${LAG} bytes (> ${MAX_LAG_FOR_FAILBACK})"
        return 1
    fi
}

########################################
# TENTAR SUBIR POSTGRESQL EM A
########################################
try_start_postgres_remote_a() {
    local PGDATA_FILES
    PGDATA_FILES=$(ssh_a "sudo find $PGDATA -mindepth 1 -maxdepth 1 2>/dev/null | wc -l" 2>/dev/null || echo "1")
    PGDATA_FILES=$(echo "$PGDATA_FILES" | tr -d ' ')
    if [ "${PGDATA_FILES:-1}" = "0" ]; then
        log_info "PGDATA em A vazio — nao tentando subir (aguarda failback com basebackup)."
        SPLITBRAIN_PROTECTED=1
        return 1
    fi
    log_info "Tentando iniciar PostgreSQL em A via SSH..."
    if ! ssh_a "sudo systemctl start $PG_SERVICE" 2>/dev/null; then
        log_warn "Falha ao iniciar PostgreSQL em A via systemctl."
        return 1
    fi
    local ATTEMPTS=0
    while [ $ATTEMPTS -lt 6 ]; do
        sleep 5
        local STATE; STATE=$(get_pg_state_remote_a)
        if [ "$STATE" != "down" ]; then
            log_ok "PostgreSQL em A iniciou como ${STATE}."
            return 0
        fi
        ATTEMPTS=$((ATTEMPTS + 1))
        log_info "Aguardando PostgreSQL em A subir (tentativa ${ATTEMPTS}/6)..."
    done
    log_warn "PostgreSQL em A nao respondeu apos 30s."
    return 1
}

########################################
# PROTEGER A CONTRA SPLIT-BRAIN
########################################
protect_node_a_from_splitbrain() {
    local STATE_A; STATE_A=$(get_pg_state_remote_a)
    if [ "$STATE_A" = "master" ]; then
        log_warn "SPLIT-BRAIN DETECTADO: A voltou como MASTER enquanto B e master!"
        log_warn "Forcando A em modo read-only para proteger dados de B..."
        local _DISK_A _DISK_B
        _DISK_A=$(get_disk_info_remote)
        _DISK_B=$(get_disk_info_local)
        send_telegram "⚡ *SPLIT-BRAIN DETECTADO*

🖥 *Node A:* ${MASTER_HOSTNAME} (${MASTER_A_IP} / ${MASTER_A_PUBLIC_IP}) — voltou como MASTER
🖥 *Node B:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP}) — MASTER ativo

🛡 *Ação automática:*
   1. Ativando read-only em A
   2. Parando PostgreSQL em A
   3. Aguardando failback para reconstruir A

📊 Disco A: ${_DISK_A}
📊 Disco B: ${_DISK_B}

🕐 Horário: $(timestamp)"

        if ssh_a 'sudo runuser -u postgres -- '"$PG_BIN"'/psql -d '"$HEALTH_DB"' -tAc "ALTER SYSTEM SET default_transaction_read_only = on; SELECT pg_reload_conf();" >/dev/null 2>&1'; then
            log_ok "A colocado em read-only. Escrita bloqueada em A."
        else
            log_warn "Nao foi possivel forcar read-only em A via ALTER SYSTEM."
        fi

        log_warn "Parando PostgreSQL em A para eliminar split-brain..."
        if ssh_a "sudo systemctl stop $PG_SERVICE" 2>/dev/null; then
            log_ok "PostgreSQL em A parado. Split-brain eliminado."
            SPLITBRAIN_PROTECTED=1
            send_telegram "✅ *SPLIT-BRAIN RESOLVIDO*

🖥 *Node A:* ${MASTER_HOSTNAME} (${MASTER_A_IP}) — PARADO
🛡 Escrita bloqueada e PostgreSQL encerrado em A

⏳ Failback será iniciado na janela: ${FAILBACK_SCHEDULE_TIME}
🕐 Horário: $(timestamp)"
            return 0
        else
            log_error "Nao foi possivel parar PostgreSQL em A."
            send_telegram "🆘 *URGENTE — SPLIT-BRAIN NÃO RESOLVIDO*

❌ Não foi possível parar o PostgreSQL em A
🖥 *Node A:* ${MASTER_A_IP} — MASTER ATIVO
🖥 *Node B:* ${SLAVE_B_IP} — MASTER ATIVO

⚠️ *DOIS MASTERS GRAVANDO SIMULTANEAMENTE*
🚨 *INTERVENÇÃO MANUAL IMEDIATA NECESSÁRIA*

Acesse ${MASTER_A_IP} e execute:
systemctl stop $PG_SERVICE

🕐 Horário: $(timestamp)"
            return 1
        fi
    fi
    return 0
}

########################################
# SAÚDE DO POSTGRESQL EM A
########################################
check_master_postgres_healthy() {
    if ! check_master_online; then
        MASTER_HEALTH_COUNT=0
        return 1
    fi
    local STATE_A; STATE_A=$(get_pg_state_remote_a)
    case "$STATE_A" in
        master)
            if ! protect_node_a_from_splitbrain; then
                MASTER_HEALTH_COUNT=0
                return 1
            fi
            STATE_A=$(get_pg_state_remote_a)
            if [ "$STATE_A" = "down" ]; then
                log_info "A parado apos protecao split-brain. Aguardando failback iniciar basebackup."
                MASTER_HEALTH_COUNT=$((MASTER_HEALTH_COUNT + 1))
                [ "$MASTER_HEALTH_COUNT" -ge "$FAILBACK_HEALTH_REQUIRED" ]
                return $?
            fi
            log_info "A online como MASTER (check ${MASTER_HEALTH_COUNT}/${FAILBACK_HEALTH_REQUIRED})"
            ;;
        replica)
            log_info "A online como REPLICA (check ${MASTER_HEALTH_COUNT}/${FAILBACK_HEALTH_REQUIRED})"
            ;;
        down)
            if [ "$SPLITBRAIN_PROTECTED" = "1" ]; then
                log_info "A parado por split-brain. Contando check para iniciar failback (${MASTER_HEALTH_COUNT}/${FAILBACK_HEALTH_REQUIRED})..."
                MASTER_HEALTH_COUNT=$((MASTER_HEALTH_COUNT + 1))
                [ "$MASTER_HEALTH_COUNT" -ge "$FAILBACK_HEALTH_REQUIRED" ]
                return $?
            fi
            log_warn "PostgreSQL em A nao esta respondendo (state=down) — tentando iniciar..."
            if try_start_postgres_remote_a; then
                STATE_A=$(get_pg_state_remote_a)
                log_ok "PostgreSQL em A subiu como ${STATE_A} apos inicializacao automatica."
                if [ "$STATE_A" = "master" ]; then
                    protect_node_a_from_splitbrain || true
                    MASTER_HEALTH_COUNT=0
                    return 1
                fi
            else
                log_warn "Nao foi possivel iniciar PostgreSQL em A automaticamente."
                MASTER_HEALTH_COUNT=0
                return 1
            fi
            ;;
    esac
    MASTER_HEALTH_COUNT=$((MASTER_HEALTH_COUNT + 1))
    [ "$MASTER_HEALTH_COUNT" -ge "$FAILBACK_HEALTH_REQUIRED" ]
}

########################################
# JANELA DE FAILBACK
########################################
is_failback_window() {
    [ "$FAILBACK_NOW" = "1" ] && return 0
    local NOW_H NOW_M NOW_MIN SCHED_H SCHED_M SCHED_MIN WINDOW_END
    NOW_H=$(date "+%H" | sed 's/^0//')
    NOW_M=$(date "+%M" | sed 's/^0//')
    NOW_MIN=$(( NOW_H * 60 + NOW_M ))
    SCHED_H=$(echo "$FAILBACK_SCHEDULE_TIME" | cut -d: -f1 | sed 's/^0//')
    SCHED_M=$(echo "$FAILBACK_SCHEDULE_TIME" | cut -d: -f2 | sed 's/^0//')
    SCHED_MIN=$(( SCHED_H * 60 + SCHED_M ))
    WINDOW_END=$(( SCHED_MIN + FAILBACK_SCHEDULE_WINDOW_MIN ))
    [ "$NOW_MIN" -ge "$SCHED_MIN" ] && [ "$NOW_MIN" -lt "$WINDOW_END" ]
}

########################################
# PRE-FLIGHT CHECKS
########################################
preflight_failback() {
    log "Executando pre-flight checks para failback..."
    local ERRORS=0

    if ! check_master_online; then
        log_error "Pre-flight: A nao responde via SSH (LAN)"
        ERRORS=$((ERRORS + 1))
    fi

    local STATE_A; STATE_A=$(get_pg_state_remote_a)
    if [ "$SPLITBRAIN_PROTECTED" = "1" ]; then
        log_ok "Pre-flight: A parado por split-brain — estado esperado para basebackup."
    elif [ "$STATE_A" = "down" ]; then
        local PGDATA_FILES
        PGDATA_FILES=$(ssh_a "sudo find $PGDATA -mindepth 1 -maxdepth 1 2>/dev/null | wc -l" 2>/dev/null || echo "1")
        PGDATA_FILES=$(echo "$PGDATA_FILES" | tr -d ' ')
        if [ "${PGDATA_FILES:-1}" = "0" ]; then
            log_ok "Pre-flight: PGDATA em A vazio — pronto para receber basebackup."
        else
            log_error "Pre-flight: PostgreSQL em A nao responde (PGDATA nao vazio ou inacessivel)"
            ERRORS=$((ERRORS + 1))
        fi
    else
        log_ok "Pre-flight: PostgreSQL em A OK (role=${STATE_A})"
    fi

    local FREE_A
    FREE_A=$(ssh_a "df --output=avail -m /var/lib/pgsql | tail -1" 2>/dev/null | tr -d ' ' || echo 0)
    if [ "${FREE_A:-0}" -lt "$DISK_MIN_FREE_MB" ]; then
        log_error "Pre-flight: Espaco em A insuficiente (${FREE_A}MB < ${DISK_MIN_FREE_MB}MB)"
        ERRORS=$((ERRORS + 1))
    else
        log_ok "Pre-flight: Espaco em A OK (${FREE_A}MB livres)"
    fi

    local FREE_B
    FREE_B=$(df --output=avail -m /var/lib/pgsql | tail -1 | tr -d ' ')
    if [ "${FREE_B:-0}" -lt "$DISK_MIN_FREE_MB" ]; then
        log_error "Pre-flight: Espaco em B insuficiente (${FREE_B}MB < ${DISK_MIN_FREE_MB}MB)"
        ERRORS=$((ERRORS + 1))
    else
        log_ok "Pre-flight: Espaco em B OK (${FREE_B}MB livres)"
    fi

    if ! check_if_promoted; then
        log_error "Pre-flight: B nao esta como master"
        ERRORS=$((ERRORS + 1))
    fi

    if [ "$SPLITBRAIN_PROTECTED" = "1" ]; then
        log_ok "Pre-flight: Conectividade A->B pulada (A parado por split-brain)."
    elif ! ssh_a "sudo runuser -u postgres -- $PG_BIN/psql \
                -h $SLAVE_B_IP -U $REPL_USER -d $HEALTH_DB \
                -c 'SELECT 1' >/dev/null 2>&1"; then
        log_error "Pre-flight: A nao consegue conectar em B — pg_basebackup vai falhar"
        ERRORS=$((ERRORS + 1))
    else
        log_ok "Pre-flight: Conectividade A->B OK"
    fi

    if [ "$SPLITBRAIN_PROTECTED" = "1" ]; then
        log_ok "Pre-flight: Verificacao de lag pulada (A parado — basebackup garantira zero data loss)."
    elif ! check_remote_lag_for_failback; then
        log_error "Pre-flight: A com lag alto — failback adiado para garantir zero data loss"
        ERRORS=$((ERRORS + 1))
    fi

    if [ "$ERRORS" -gt 0 ]; then
        log_error "Pre-flight falhou com $ERRORS erro(s). Failback abortado."
        send_telegram "⚠️ *FAILBACK ABORTADO*

❌ Pre-flight falhou com *${ERRORS} erro(s)*
🖥 Host: ${SLAVE_HOSTNAME} (${SLAVE_B_IP})

🔍 Verifique: /var/log/failover.log
🕐 Horário: $(timestamp)"
        return 1
    fi
    log_ok "Pre-flight OK — todos os checks passaram."
    return 0
}

########################################
# GARANTE POSTGRESQL RODANDO (local B)
########################################
ensure_postgres_running() {
    systemctl is-active --quiet $PG_SERVICE && return 0
    log_warn "PostgreSQL inativo. Tentando iniciar..."
    if [ -f "$PGDATA/standby.signal" ] && check_master_online; then
        ensure_replica_config
    fi
    systemctl start $PG_SERVICE >> "$LOGFILE" 2>&1
    sleep 5
    if systemctl is-active --quiet $PG_SERVICE; then
        log_ok "PostgreSQL iniciou com sucesso."
        return 0
    fi
    log_error "PostgreSQL falhou ao iniciar."
    local _DISK_B _MEM_B _LOAD_B
    _DISK_B=$(get_disk_info_local); _MEM_B=$(get_mem_info_local); _LOAD_B=$(get_load_local)
    send_telegram "🚨 *POSTGRESQL NÃO SUBIU*

🖥 *Host:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP})

📊 *Métricas no momento da falha:*
   💾 Disco B: ${_DISK_B}
   🧠 Memória B: ${_MEM_B}
   ⚡ Load B: ${_LOAD_B}

🔍 journalctl -u $PG_SERVICE -n 50
🕐 Horário: $(timestamp)"
    exit 1
}

########################################
# GARANTIR B CONFIGURADO COMO REPLICA
########################################
ensure_replica_config() {
    log_info "Verificando configuracao de replica em B..."
    if [ ! -f "$PGDATA/standby.signal" ]; then
        log_warn "standby.signal ausente — recriando..."
        touch "$PGDATA/standby.signal"
        chown postgres:postgres "$PGDATA/standby.signal"
    fi
    if ! grep -q "primary_conninfo" "$PGDATA/postgresql.auto.conf" 2>/dev/null; then
        log_warn "primary_conninfo ausente — configurando..."
        cat >> "$PGDATA/postgresql.auto.conf" << EOF
primary_conninfo = 'host=${MASTER_A_IP} port=5432 user=${REPL_USER} password=${REPL_PASSWORD} application_name=nodeB'
primary_slot_name = '${MY_SLOT}'
recovery_target_timeline = 'latest'
EOF
        chown postgres:postgres "$PGDATA/postgresql.auto.conf"
    fi
    log_ok "Configuracao de replica OK."
}

########################################
# PGPASS
########################################
setup_pgpass_local() {
    cat > /var/lib/pgsql/.pgpass << EOF
*:*:*:$REPL_USER:$REPL_PASSWORD
EOF
    chown postgres:postgres /var/lib/pgsql/.pgpass
    chmod 600 /var/lib/pgsql/.pgpass
    log_ok "pgpass local configurado"
}

setup_pgpass_remote_a() {
    log "Configurando pgpass em A (${MASTER_HOSTNAME})..."
    echo "*:*:*:$REPL_USER:$REPL_PASSWORD" | ssh_a "
        cat > /tmp/pgpass_tmp
        chown postgres:postgres /tmp/pgpass_tmp
        chmod 600 /tmp/pgpass_tmp
        mv /tmp/pgpass_tmp /var/lib/pgsql/.pgpass
    "
}

########################################
# REPLICATION SLOTS
########################################
ensure_replication_slot_local() {
    local SLOT="$1"
    log "Garantindo slot '$SLOT' em B..."
    runuser -u postgres -- "$PG_BIN/psql" \
        -d "$HEALTH_DB" \
        -v ON_ERROR_STOP=1 <<SQL
DO \$\$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT'
    ) THEN PERFORM pg_create_physical_replication_slot('$SLOT'); END IF;
END \$\$;
SQL
}

ensure_replication_slot_remote_a() {
    local SLOT="$1"
    log "Garantindo slot '$SLOT' em A..."
    ssh_a "
        for i in \$(seq 1 30); do
            sudo runuser -u postgres -- $PG_BIN/psql \
                -d $HEALTH_DB -c 'SELECT 1' >/dev/null 2>&1 && break
            sleep 1
        done
        sudo runuser -u postgres -- $PG_BIN/psql \
            -d $HEALTH_DB -v ON_ERROR_STOP=1 <<'SQL'
DO \$\$ BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_replication_slots WHERE slot_name = '$SLOT'
    ) THEN PERFORM pg_create_physical_replication_slot('$SLOT'); END IF;
END \$\$;
SQL
    "
}

########################################
# ASTERISK / PROCESSOS .RB
########################################
check_asterisk() {
    log "Parando Asterisk antes do failback..."
    if pgrep -x asterisk >/dev/null 2>&1; then
        asterisk -rx 'core stop now' >> "$LOGFILE" 2>&1 \
            || log_warn "core stop now falhou (continuando)"
    else
        log_info "Asterisk nao estava rodando."
    fi
}

pkill_if_recovery() {
    local STATE; STATE=$(get_pg_state_local)
    case "$STATE" in
        replica)
            log "No SLAVE — encerrando processos .rb"
            pkill -f "/home/totalip/ipserver/.*\.rb" || true
            ;;
        master)
            log_ok "No MASTER — processos .rb mantidos."
            ;;
        *)
            log_warn "Estado indefinido — encerrando processos .rb por seguranca"
            pkill -f "/home/totalip/ipserver/.*\.rb" || true
            ;;
    esac
}

########################################
# LIMPEZA DE BACKUPS
########################################
cleanup_old_db_backup() {
    [ ! -d "$BACKUP_DB_DIR" ] && return 0
    local HOJE; HOJE=$(date +%Y-%m-%d)
    local PREFIXOS
    PREFIXOS=$(find "$BACKUP_DB_DIR" -maxdepth 1 -type f \
        \( -name 'db-*' -o -name 'db_*' \) \
        | sed -E 's#.*/(db.*)-[0-9]{4}-[0-9]{2}-[0-9]{2}$#\1#' | sort -u)
    [ -z "$PREFIXOS" ] && return 0
    for PREFIX in $PREFIXOS; do
        local ARQ_HOJE="${BACKUP_DB_DIR}/${PREFIX}-${HOJE}"
        [ ! -f "$ARQ_HOJE" ] && continue
        find "$BACKUP_DB_DIR" -maxdepth 1 -type f \
            \( -name "${PREFIX}-*" -o -name "${PREFIX}_*" \) \
            ! -name "*-${HOJE}" | while read -r f; do
                log "Removendo backup antigo: $(basename "$f")"
                rm -f "$f"
            done
    done
}

########################################
# ROLLBACK
########################################
rollback_failback() {
    log_error "ROLLBACK: tentando restaurar B como master..."
    local _DISK_B _MEM_B _LOAD_B
    _DISK_B=$(get_disk_info_local); _MEM_B=$(get_mem_info_local); _LOAD_B=$(get_load_local)
    send_telegram "🚨 *FAILBACK FALHOU — ROLLBACK*

⚠️ Falha durante o processo de failback
🔄 Restaurando B como master...

🖥 *Node B:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP})
📊 *Métricas:*
   💾 Disco B: ${_DISK_B}
   🧠 Memória B: ${_MEM_B}
   ⚡ Load B: ${_LOAD_B}

🔍 Verifique: /var/log/failover.log
🕐 Horário: $(timestamp)"

    systemctl stop $PG_SERVICE 2>/dev/null || true
    sleep 3
    systemctl start $PG_SERVICE >> "$LOGFILE" 2>&1 || true
    sleep 5

    local STATE; STATE=$(get_pg_state_local)
    if [ "$STATE" = "replica" ]; then
        log_warn "Rollback: B subiu como slave. Promovendo de volta..."
        runuser -u postgres -- "$PG_BIN/pg_ctl" -D "$PGDATA" promote \
            >> "$LOGFILE" 2>&1 || true
        sleep 5
    fi

    pkill_if_recovery
    echo "$(timestamp) rollback" > "$FAILBACK_ERROR_FLAG"

    send_telegram "🔁 *ROLLBACK CONCLUÍDO*

🖥 *B operando como master:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP})

🚨 *INTERVENÇÃO MANUAL NECESSÁRIA*
   Para desbloquear: truncate -s0 /var/lib/pgsql/.failback_error

🕐 Horário: $(timestamp)"
}

########################################
# FAILBACK PRINCIPAL
########################################
do_failback() {
    log "Iniciando FAILBACK — A volta como master, B vira slave"
    FAILBACK_START_TS=$(date +%s)

    local _DISK_B _DISK_A _MEM_B _LOAD_B
    _DISK_B=$(get_disk_info_local); _DISK_A=$(get_disk_info_remote)
    _MEM_B=$(get_mem_info_local);   _LOAD_B=$(get_load_local)
    send_telegram "🔄 *FAILBACK INICIANDO*

🎯 *Objetivo:* Restaurar A como master

🖥 *Node A:* ${MASTER_HOSTNAME} (${MASTER_A_IP} / ${MASTER_A_PUBLIC_IP}) — voltando
🖥 *Node B:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP} / ${SLAVE_B_PUBLIC_IP}) — master atual

📊 *Métricas pré-failback:*
   💾 Disco A: ${_DISK_A}
   💾 Disco B: ${_DISK_B}
   🧠 Memória B: ${_MEM_B}
   ⚡ Load B: ${_LOAD_B}

🕐 Horário: $(timestamp)"

    preflight_failback || return 1

    setup_pgpass_local
    setup_pgpass_remote_a
    ensure_replication_slot_local "$REMOTE_SLOT"

    log "  [1/6] Parando PostgreSQL em A..."
    if ! ssh_a_retry "sudo systemctl stop $PG_SERVICE || true"; then
        log_error "Nao foi possivel executar stop em A"
        rollback_failback; return 1
    fi
    log_ok "  [1/6] PostgreSQL em A parado."

    log "  [2/6] Limpando PGDATA em A..."
    if [ -z "$PGDATA" ] || [ "$PGDATA" = "/" ]; then
        log_error "PGDATA invalido ('$PGDATA') — abortando"
        rollback_failback; return 1
    fi
    if ! ssh_a "sudo rm -rf $PGDATA && \
                sudo mkdir -p $PGDATA && \
                sudo chown postgres:postgres $PGDATA && \
                sudo chmod 700 $PGDATA"; then
        log_error "Falha ao limpar PGDATA em A"
        rollback_failback; return 1
    fi
    log_ok "  [2/6] PGDATA em A limpo."

    log "  [3/6] pg_basebackup B -> A (timeout: ${BASEBACKUP_TIMEOUT}s)..."
    if ! ssh_a "sudo runuser -u postgres -- /bin/timeout $BASEBACKUP_TIMEOUT \
                $PG_BIN/pg_basebackup \
                -h $SLAVE_B_IP -D $PGDATA -U $REPL_USER \
                -P -v -R --wal-method=stream --slot=$REMOTE_SLOT" \
                2>&1 | tee -a "$LOGFILE" ; then
        log_error "pg_basebackup B->A falhou"
        rollback_failback; return 1
    fi
    log_ok "  [3/6] pg_basebackup B->A concluido."

    log "  [4/6] Iniciando PostgreSQL em A e promovendo..."
    if ! ssh_a "sudo systemctl start $PG_SERVICE"; then
        log_error "Nao foi possivel iniciar PostgreSQL em A"
        rollback_failback; return 1
    fi
    sleep 5
    if ! ssh_a "sudo runuser -u postgres -- $PG_BIN/pg_ctl -D $PGDATA promote"; then
        log_error "Promocao de A falhou"
        rollback_failback; return 1
    fi

    local A_STATE=""
    local ATTEMPTS=0
    while [ $ATTEMPTS -lt 12 ]; do
        A_STATE=$(get_pg_state_remote_a)
        [ "$A_STATE" = "master" ] && break
        ATTEMPTS=$((ATTEMPTS + 1))
        log_info "  [4/6] Aguardando A virar master (tentativa $ATTEMPTS/12, state=$A_STATE)..."
        sleep 5
    done
    if [ "$A_STATE" != "master" ]; then
        log_error "A nao assumiu como master apos promocao (state=$A_STATE)"
        rollback_failback; return 1
    fi
    log_ok "  [4/6] A promovido como master."

    log_info "  [4/6] Executando check_system em A..."
    run_check_system_remote_a
    ensure_replication_slot_remote_a "$MY_SLOT"
    sleep 10

    log "  [5/6] Reconstruindo B como slave..."
    check_asterisk
    systemctl stop $PG_SERVICE
    if [ -z "$PGDATA" ] || [ "$PGDATA" = "/" ]; then
        log_error "PGDATA invalido ('$PGDATA') — abortando rm -rf"
        rollback_failback; return 1
    fi
    rm -rf "$PGDATA"
    mkdir -p "$PGDATA"
    chown postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"

    if ! timeout "$BASEBACKUP_TIMEOUT" \
         runuser -u postgres -- "$PG_BIN/pg_basebackup" \
            -h "$MASTER_A_IP" -D "$PGDATA" -U "$REPL_USER" \
            -P -v -R --wal-method=stream --slot="$MY_SLOT" \
            2>&1 | tee -a "$LOGFILE" ; then
        log_error "pg_basebackup A->B falhou"
        rollback_failback; return 1
    fi
    log_ok "  [5/6] pg_basebackup A->B concluido."

    log "  [6/6] Iniciando PostgreSQL em B como slave..."
    systemctl start $PG_SERVICE
    sleep 5

    local B_STATE; B_STATE=$(get_pg_state_local)
    if [ "$B_STATE" != "replica" ]; then
        log_error "B nao iniciou como slave apos failback (state=$B_STATE)"
        rollback_failback; return 1
    fi
    log_ok "  [6/6] B iniciado como slave."

    pkill_if_recovery
    MASTER_HEALTH_COUNT=0
    SPLITBRAIN_PROTECTED=0
    DRG_ALERT_COUNT=0
    echo "$(timestamp)" > "$FLAG_FAILBACK"
    log_ok "FAILBACK concluido com sucesso"

    local _DUR_FB _MEM_A _LOAD_A
    _DUR_FB=$(calc_duration "$FAILBACK_START_TS")
    _DISK_A=$(get_disk_info_remote); _DISK_B=$(get_disk_info_local)
    _MEM_A=$(get_mem_info_remote);   _LOAD_A=$(get_load_remote)
    send_telegram "✅ *FAILBACK CONCLUÍDO*

👑 *Master restaurado:* ${MASTER_HOSTNAME} (${MASTER_A_IP} / ${MASTER_A_PUBLIC_IP})
🔄 *Replica:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP} / ${SLAVE_B_PUBLIC_IP})

⏱ *Duração do failback:* ${_DUR_FB}
📊 *Métricas pós-failback:*
   💾 Disco A: ${_DISK_A}
   💾 Disco B: ${_DISK_B}
   🧠 Memória A: ${_MEM_A}
   ⚡ Load A: ${_LOAD_A}

🕐 Horário: $(timestamp)"
}

########################################
# FAILOVER PRINCIPAL
########################################
do_failover() {
    log "Promovendo B (${SLAVE_HOSTNAME}) para master..."
    ensure_postgres_running

    if ! check_local_lag_acceptable; then
        log_warn "B com lag alto — promovendo mesmo assim (A confirmado offline em LAN e rede pública)"
        local _LAG_B; _LAG_B=$(get_local_wal_lag_bytes 2>/dev/null || echo "N/A")
        send_telegram "⚠️ *FAILOVER COM LAG ALTO*

🖥 *Node B:* ${SLAVE_HOSTNAME}
📡 *Lag WAL de B:* ${_LAG_B} bytes (limite: ${MAX_LAG_FOR_FAILOVER})

⚠️ A está offline (LAN + rede pública) — promovendo B mesmo assim
🔍 Verifique possível perda de dados após recuperação

🕐 Horário: $(timestamp)"
    fi

    runuser -u postgres -- "$PG_BIN/pg_ctl" -D "$PGDATA" promote \
        >> "$LOGFILE" 2>&1 || log_error "Erro durante promocao de B"

    log "Aguardando estabilizacao..."
    local i STATE_NOW
    for i in $(seq 1 15); do
        STATE_NOW=$(get_pg_state_local)
        [ "$STATE_NOW" = "master" ] && break
        sleep 2
    done

    local FINAL_STATE; FINAL_STATE=$(get_pg_state_local)
    if [ "$FINAL_STATE" != "master" ]; then
        log_error "B nao assumiu como master apos promocao (state=$FINAL_STATE)"
        local _DISK_B _MEM_B _LOAD_B
        _DISK_B=$(get_disk_info_local); _MEM_B=$(get_mem_info_local); _LOAD_B=$(get_load_local)
        send_telegram "🚨 *FALHA NO FAILOVER*

❌ B não conseguiu se promover a master
🖥 *Node B:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP})

📊 *Métricas:*
   💾 Disco B: ${_DISK_B}
   🧠 Memória B: ${_MEM_B}
   ⚡ Load B: ${_LOAD_B}

🚨 *INTERVENÇÃO MANUAL NECESSÁRIA*
   Verifique: /var/log/failover.log
🕐 Horário: $(timestamp)"
        return 1
    fi

    log_ok "Failover OK. Subindo servicos em B..."
    run_check_system_local

    local _DUR_FO _DISK_B _MEM_B _LOAD_B _LAG
    _DUR_FO=$(calc_duration "$FAILOVER_START_TS")
    _DISK_B=$(get_disk_info_local); _MEM_B=$(get_mem_info_local); _LOAD_B=$(get_load_local)
    _LAG=$(get_local_wal_lag_bytes 2>/dev/null || echo "N/A")
    send_telegram "✅ *FAILOVER CONCLUÍDO*

🖥 *Novo Master:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP} / ${SLAVE_B_PUBLIC_IP})
🔌 *Master anterior:* ${MASTER_HOSTNAME} (${MASTER_A_IP} / ${MASTER_A_PUBLIC_IP}) — OFFLINE

⏱ *Duração do failover:* ${_DUR_FO}
📊 *Métricas pós-failover:*
   💾 Disco B: ${_DISK_B}
   🧠 Memória B: ${_MEM_B}
   ⚡ Load B: ${_LOAD_B}
   📡 Lag WAL: ${_LAG} bytes

🕐 Horário: $(timestamp)"
}

########################################
# MAIN LOOP
########################################
log "pg-failover iniciado (PID $$) — systemd managed"
log "  No local  : ${SLAVE_HOSTNAME} (${SLAVE_B_IP} / pub: ${SLAVE_B_PUBLIC_IP})"
log "  No remoto : ${MASTER_A_IP} (pub: ${MASTER_A_PUBLIC_IP})"
log "  Failover threshold : ${FAILOVER_THRESHOLD} falhas consecutivas (LAN + rede publica)"
log "  Failback agendado  : ${FAILBACK_SCHEDULE_TIME} (+${FAILBACK_SCHEDULE_WINDOW_MIN}min)"
log "  Failback imediato  : FAILBACK_NOW=${FAILBACK_NOW}"
log "  Max lag failover   : ${MAX_LAG_FOR_FAILOVER} bytes"
log "  Max lag failback   : ${MAX_LAG_FOR_FAILBACK} bytes"
log "  Health DB          : ${HEALTH_DB} (socket Unix)"
log "  Env file           : ${ENV_FILE}"
cleanup_old_db_backup

LAST_STATE=$(get_pg_state_local)
log "  Estado inicial de B: ${LAST_STATE}"
pkill_if_recovery

while true; do

    ensure_postgres_running

    # ------------------------------------------------------------------ #
    # MASTER A ONLINE
    # ------------------------------------------------------------------ #
    if check_master_online; then
        MASTER_HOSTNAME=$(get_master_hostname)   # SSH só quando A responde
        FAILURE_COUNT=0
        DRG_ALERT_COUNT=0
        LOCAL_STATE=$(get_pg_state_local)

        if [ -s "$FLAG_FAILOVER" ]; then
            log_info "A (${MASTER_HOSTNAME}) voltou online. Reiniciando ciclo de failback."
            _DISK_A=$(get_disk_info_remote); _MEM_A=$(get_mem_info_remote); _LOAD_A=$(get_load_remote)
            _STATE_A=$(get_pg_state_remote_a)
            send_telegram "🟢 *MASTER A VOLTOU ONLINE*

🖥 *Node A:* ${MASTER_HOSTNAME} (${MASTER_A_IP} / ${MASTER_A_PUBLIC_IP})
   PostgreSQL: ${_STATE_A}
📊 *Métricas:*
   💾 Disco A: ${_DISK_A}
   🧠 Memória A: ${_MEM_A}
   ⚡ Load A: ${_LOAD_A}

⏳ Aguardando janela de failback: ${FAILBACK_SCHEDULE_TIME}
🕐 Horário: $(timestamp)"
            > "$FLAG_FAILOVER"
            > "$FLAG_FAILBACK"
            MASTER_HEALTH_COUNT=0
        fi

        if [ "$LOCAL_STATE" = "master" ]; then
            if [ -s "$FAILBACK_ERROR_FLAG" ]; then
                log_warn "Failback anterior com erro — intervencao manual necessaria"
                log_warn "Para desbloquear: truncate -s0 $FAILBACK_ERROR_FLAG"
                sleep "$CHECK_INTERVAL"
                continue
            fi
            if [ -s "$FLAG_FAILBACK" ]; then
                log_info "Failback ja concluido neste ciclo."
                sleep "$CHECK_INTERVAL"
                continue
            fi
            if check_master_postgres_healthy; then
                if is_failback_window; then
                    log "Janela de failback ativa (${FAILBACK_SCHEDULE_TIME}) — iniciando failback..."
                    do_failback
                else
                    log_info "A saudavel (${MASTER_HEALTH_COUNT}/${FAILBACK_HEALTH_REQUIRED}) | Aguardando janela ${FAILBACK_SCHEDULE_TIME}"
                fi
            fi
        fi

        if [ "$LOCAL_STATE" = "replica" ]; then
            HEARTBEAT_COUNT=$(( ${HEARTBEAT_COUNT:-0} + 1 ))
            if [ "$HEARTBEAT_COUNT" -ge 12 ]; then
                HEARTBEAT_COUNT=0
                _LAG=$(get_local_wal_lag_bytes 2>/dev/null || echo "N/A")
                log_info "STATUS OK | A=master (${MASTER_HOSTNAME}) | B=replica | lag=${_LAG} bytes"
            fi
            {
                _NOW_H=$(date "+%H" | sed 's/^0*//' | grep -v '^$' || echo "0")
                _NOW_M=$(date "+%M" | sed 's/^0*//' | grep -v '^$' || echo "0")
                _TODAY=$(date "+%Y-%m-%d")
                if [ "$_NOW_H" -eq 22 ] && [ "$_NOW_M" -lt 2 ] && \
                   [ "${LAST_DAILY_REPORT:-}" != "$_TODAY" ]; then
                    LAST_DAILY_REPORT="$_TODAY"
                    send_status_report "Relatório Diário 22h" || true
                fi
            } 2>/dev/null || true
        fi

    # ------------------------------------------------------------------ #
    # MASTER A OFFLINE — verifica LAN + pública antes de failover
    # ------------------------------------------------------------------ #
    else
        MASTER_HEALTH_COUNT=0

        if master_confirmed_offline; then

            if [ -s "$FLAG_FAILBACK" ]; then
                log_info "A caiu novamente apos failback anterior. Reiniciando ciclo."
                > "$FLAG_FAILBACK"
            fi

            if [ "$FAILURE_COUNT" -gt "$FAILOVER_THRESHOLD" ]; then
                FAILURE_COUNT=$FAILOVER_THRESHOLD
            fi

            if [ ! -s "$FLAG_FAILOVER" ]; then
                FAILOVER_START_TS=$(date +%s)
                _DISK_B=$(get_disk_info_local); _MEM_B=$(get_mem_info_local); _LOAD_B=$(get_load_local)
                send_telegram "🔴 *FAILOVER INICIADO*

🖥 *Master A:* ${MASTER_HOSTNAME} (${MASTER_A_IP} / ${MASTER_A_PUBLIC_IP})
   Status: OFFLINE confirmado (LAN + rede pública — ${FAILOVER_THRESHOLD}/${FAILOVER_THRESHOLD} checks)

🔄 *Promovendo B:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP})

📊 *Métricas de B:*
   💾 Disco: ${_DISK_B}
   🧠 Memória: ${_MEM_B}
   ⚡ Load: ${_LOAD_B}

🕐 Horário: $(timestamp)"
                echo "$(timestamp)" > "$FLAG_FAILOVER"
            fi

            if check_if_promoted; then
                log_ok "B (${SLAVE_HOSTNAME}) ja e Master."
            else
                do_failover
            fi
        fi
    fi

    CURRENT_STATE=$(get_pg_state_local)
    if [ "$CURRENT_STATE" != "$LAST_STATE" ]; then
        log "Mudanca de estado em B: ${LAST_STATE} -> ${CURRENT_STATE}"
        pkill_if_recovery
        LAST_STATE="$CURRENT_STATE"
    fi

    sleep "$CHECK_INTERVAL"
done