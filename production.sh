#!/bin/bash
# pg-failover.sh — Failover + Failback automático PostgreSQL HA (hardened v2)
# Instalado em: nó B (10.11.12.239)
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

set -uo pipefail

########################################
# CARREGAR CREDENCIAIS DE ARQUIVO .ENV
# Crie /etc/pg-failover.env com permissão 600
# Exemplo de conteúdo:
#   REPL_PASSWORD='totalipHa'
#   TELEGRAM_BOT_TOKEN="xxx"
#   TELEGRAM_CHAT_ID="yyy"
########################################
ENV_FILE="/etc/pg-failover.env"
if [[ -f "$ENV_FILE" ]]; then
    # shellcheck disable=SC1090
    source "$ENV_FILE"
else
    # Fallback para variáveis inline (menos seguro)
    REPL_PASSWORD="${REPL_PASSWORD:-totalipHa}"
    TELEGRAM_BOT_TOKEN="${TELEGRAM_BOT_TOKEN:-8530438377:AAFRs73B25PMMHKeT-5PbjAj-IF6ZzrjBas}"
    TELEGRAM_CHAT_ID="${TELEGRAM_CHAT_ID:-1144847708}"
fi

########################################
# CONFIG
########################################
MASTER_A_IP="10.11.12.81"
SLAVE_B_IP="10.11.12.239"

PGDATA="/var/lib/pgsql/16/data"
PG_BIN="/usr/pgsql-16/bin"

REPL_USER="replytotalip"
DB_NAME="totalipdb"

# Banco para health checks — sempre existe, funciona em master e replica
HEALTH_DB="postgres"

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

BACKUP_DB_DIR="/backup/backup_db"

#--------------------------------------
# TUNABLES
#--------------------------------------
# Quantas falhas consecutivas do health check triplo antes de failover
FAILOVER_THRESHOLD=3

# Quantos checks saudáveis consecutivos de A antes de permitir failback
FAILBACK_HEALTH_REQUIRED=4

# Intervalo entre checks (segundos)
CHECK_INTERVAL=5

# Timeout para pg_basebackup (segundos)
BASEBACKUP_TIMEOUT=3600

# Espaço mínimo livre em /var/lib/pgsql (MB)
DISK_MIN_FREE_MB=5120

# Lag máximo aceitável para failover (bytes)
# Se a replica estiver mais atrasada que isso, não promove
# 0 = não verificar lag (failover imediato mesmo com dados faltando)
MAX_LAG_FOR_FAILOVER=1048576   # 1MB

# Lag máximo aceitável para failback (bytes)
# node1 precisa estar abaixo deste valor antes de executar o failback
MAX_LAG_FOR_FAILBACK=1048576   # 1MB

#--------------------------------------
# FAILBACK AGENDADO
#--------------------------------------
FAILBACK_SCHEDULE_TIME="02:00"
FAILBACK_SCHEDULE_WINDOW_MIN=30

# FAILBACK_NOW=1 → força failback imediato ignorando horário
# Recomendação: em produção, use FAILBACK_NOW=1 manualmente
# e mantenha FAILBACK_NOW=0 no script (evita failback automático inesperado)
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
SPLITBRAIN_PROTECTED=0   # 1 = A foi parado por split-brain, nao tentar subir
SLAVE_HOSTNAME=$(hostname)
MASTER_HOSTNAME="unknown"

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
# METRICAS DO SISTEMA — para Telegram
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

get_pg_version_local() {
    runuser -u postgres -- "$PG_BIN/psql" -d "$HEALTH_DB" \
        -tAc "SELECT version();" 2>/dev/null | awk '{print $1, $2}' || echo "N/A"
}

# Calcula duracao em formato legivel (ex: 2h 15m 30s)
calc_duration() {
    local START="$1"
    local END
    END=$(date +%s)
    local DIFF=$(( END - START ))
    local H=$(( DIFF / 3600 ))
    local M=$(( (DIFF % 3600) / 60 ))
    local S=$(( DIFF % 60 ))
    if [ $H -gt 0 ]; then
        echo "${H}h ${M}m ${S}s"
    elif [ $M -gt 0 ]; then
        echo "${M}m ${S}s"
    else
        echo "${S}s"
    fi
}

# Variaveis de tempo para medir duracao
FAILOVER_START_TS=0
FAILBACK_START_TS=0

########################################
# SSH
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

get_master_hostname() {
    MASTER_HOSTNAME=$(ssh_a "hostname" 2>/dev/null || echo "unknown")
    echo "$MASTER_HOSTNAME"
}

########################################
# HEALTH CHECK TRIPLO: ping + SSH + PostgreSQL
#
# Melhoria 1: apenas SSH não é suficiente
# SSH pode cair por timeout mas PostgreSQL estar OK,
# ou o servidor pode estar em estado zumbi (kernel panic parcial)
# O triplo check reduz drasticamente falsos positivos
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

# check_master_online: usado para verificar se A está acessível
# Usa ping + SSH (rápido, para o loop principal)
check_master_online() {
    check_ping_a && check_ssh_a
}

# master_confirmed_offline: usado para disparar o failover
# Requer FAILOVER_THRESHOLD falhas consecutivas do health check triplo
# Melhoria: inclui verificação de PostgreSQL para evitar failover
# quando só o SSH caiu mas o banco está OK
master_confirmed_offline() {
    local PING_OK=0 SSH_OK=0 PG_OK=0

    check_ping_a && PING_OK=1
    check_ssh_a  && SSH_OK=1
    [ $SSH_OK -eq 1 ] && check_pg_a && PG_OK=1

    if [ $PING_OK -eq 1 ] || [ $SSH_OK -eq 1 ] || [ $PG_OK -eq 1 ]; then
        # Pelo menos um check passou — A não está totalmente offline
        if [ $FAILURE_COUNT -gt 0 ]; then
            log_info "A respondeu parcialmente (ping=$PING_OK ssh=$SSH_OK pg=$PG_OK). Resetando contador."
        fi
        FAILURE_COUNT=0
        return 1
    fi

    FAILURE_COUNT=$((FAILURE_COUNT + 1))
    log_warn "A offline — ping=$PING_OK ssh=$SSH_OK pg=$PG_OK (falha ${FAILURE_COUNT}/${FAILOVER_THRESHOLD})"
    [ "$FAILURE_COUNT" -ge "$FAILOVER_THRESHOLD" ]
}

########################################
# ESTADO POSTGRESQL LOCAL (B)
# Socket Unix — sem senha, sem -h, sem -U
########################################
get_pg_state_local() {
    local RESULT
    RESULT=$(runuser -u postgres -- "$PG_BIN/psql" \
        -d "$HEALTH_DB" \
        -tAc "SELECT pg_is_in_recovery();" 2>/dev/null | tr -d '[:space:]')
    case "$RESULT" in
        f) echo "master"  ;;
        t) echo "replica" ;;
        *) echo "down"    ;;
    esac
}

get_pg_state() {
    local S
    S=$(get_pg_state_local)
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
# Socket Unix via SSH + sudo — aceita master ou replica
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
#
# Melhoria 4: verificar lag antes do failover
# Promover replica atrasada = perda de dados garantida
########################################
get_local_wal_lag_bytes() {
    # Diferença entre WAL recebido e WAL aplicado (replay)
    # 0 = totalmente sincronizado
    runuser -u postgres -- "$PG_BIN/psql" \
        -d "$HEALTH_DB" \
        -tAc "SELECT COALESCE(
                pg_wal_lsn_diff(pg_last_wal_receive_lsn(), pg_last_wal_replay_lsn()),
                0
              );" 2>/dev/null | tr -d '[:space:]' || echo "999999999"
}

check_local_lag_acceptable() {
    # Verifica se B está suficientemente sincronizado para ser promovido
    if [ "$MAX_LAG_FOR_FAILOVER" -eq 0 ]; then
        log_info "Verificacao de lag desabilitada (MAX_LAG_FOR_FAILOVER=0)"
        return 0
    fi

    local LAG
    LAG=$(get_local_wal_lag_bytes)

    # Se não conseguiu obter o lag, assumir que está ok (B pode estar em modo master)
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
    # Lag de A como replica de B (durante FAILBACK_PENDING)
    # Retorna bytes de diferença entre B (master) e A (replica)
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
    # Verifica se A está sincronizado antes do failback
    if [ "$MAX_LAG_FOR_FAILBACK" -eq 0 ]; then
        log_info "Verificacao de lag para failback desabilitada"
        return 0
    fi

    local LAG
    LAG=$(get_remote_replication_lag)

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
# Chamado quando A responde SSH mas PostgreSQL está down
########################################
try_start_postgres_remote_a() {
    log_info "Tentando iniciar PostgreSQL em A via SSH..."

    if ! ssh_a "sudo systemctl start postgresql-16" 2>/dev/null; then
        log_warn "Falha ao iniciar PostgreSQL em A via systemctl."
        return 1
    fi

    # Aguardar PostgreSQL aceitar conexões (até 30s)
    local ATTEMPTS=0
    while [ $ATTEMPTS -lt 6 ]; do
        sleep 5
        local STATE
        STATE=$(get_pg_state_remote_a)
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
#
# Quando A volta online enquanto B é master, A pode voltar como master
# se o PostgreSQL foi iniciado manualmente ou se tinha dados anteriores.
# Isso causa split-brain: dois masters gravando ao mesmo tempo.
#
# Solução: assim que A é detectado online, forçar read-only em A
# e garantir que ele esteja como replica antes do failback.
########################################
protect_node_a_from_splitbrain() {
    local STATE_A
    STATE_A=$(get_pg_state_remote_a)

    if [ "$STATE_A" = "master" ]; then
        log_warn "SPLIT-BRAIN DETECTADO: A voltou como MASTER enquanto B e master!"
        log_warn "Forcando A em modo read-only para proteger dados de B..."
        local _DISK_A _DISK_B
        _DISK_A=$(get_disk_info_remote)
        _DISK_B=$(get_disk_info_local)
        send_telegram "⚡ *SPLIT-BRAIN DETECTADO*

🖥 *Node A:* ${MASTER_HOSTNAME} (${MASTER_A_IP}) — voltou como MASTER
🖥 *Node B:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP}) — MASTER ativo

🛡 *Ação automática:*
   1. Ativando read-only em A
   2. Parando PostgreSQL em A
   3. Aguardando failback para reconstruir A

📊 Disco A: ${_DISK_A}
📊 Disco B: ${_DISK_B}

🕐 Horário: $(timestamp)"

        # 1. Forçar read-only em A via ALTER SYSTEM
        if ssh_a 'sudo runuser -u postgres -- '"$PG_BIN"'/psql -d '"$HEALTH_DB"' -tAc "ALTER SYSTEM SET default_transaction_read_only = on; SELECT pg_reload_conf();" >/dev/null 2>&1'; then
            log_ok "A colocado em read-only. Escrita bloqueada em A."
        else
            log_warn "Nao foi possivel forcar read-only em A via ALTER SYSTEM."
        fi

        # 2. Tentar fazer pg_ctl stop graceful em A para eliminar o risco
        log_warn "Parando PostgreSQL em A para eliminar split-brain..."
        if ssh_a "sudo systemctl stop postgresql-16" 2>/dev/null; then
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
            log_error "INTERVENCAO MANUAL NECESSARIA em ${MASTER_A_IP}"
            send_telegram "🆘 *URGENTE — SPLIT-BRAIN NÃO RESOLVIDO*

❌ Não foi possível parar o PostgreSQL em A
🖥 *Node A:* ${MASTER_A_IP} — MASTER ATIVO
🖥 *Node B:* ${SLAVE_B_IP} — MASTER ATIVO

⚠️ *DOIS MASTERS GRAVANDO SIMULTANEAMENTE*
🚨 *INTERVENÇÃO MANUAL IMEDIATA NECESSÁRIA*

Acesse ${MASTER_A_IP} e execute:
systemctl stop postgresql-16

🕐 Horário: $(timestamp)"
            return 1
        fi
    fi

    return 0
}

########################################
# SAÚDE DO POSTGRESQL EM A
# Aceita A como master OU replica
# Se PostgreSQL estiver down mas SSH OK, tenta subir automaticamente
########################################
check_master_postgres_healthy() {
    if ! check_master_online; then
        MASTER_HEALTH_COUNT=0
        return 1
    fi

    local STATE_A
    STATE_A=$(get_pg_state_remote_a)

    case "$STATE_A" in
        master)
            # A voltou como master enquanto B é master — split-brain!
            # Proteger imediatamente antes de prosseguir
            if ! protect_node_a_from_splitbrain; then
                MASTER_HEALTH_COUNT=0
                return 1
            fi
            # Após proteção A está parado — tentar subir como replica
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
            # Se A foi parado por protecao split-brain, nao tentar subir
            # O failback fara o basebackup com A parado (estado correto)
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
                # Se subiu como master, proteger imediatamente
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

    # 1. A responde via SSH
    if ! check_master_online; then
        log_error "Pre-flight: A nao responde via SSH"
        ERRORS=$((ERRORS + 1))
    fi

    # 2. PostgreSQL em A
    # Se SPLITBRAIN_PROTECTED=1, A foi parado intencionalmente — estado esperado
    # O pg_basebackup vai reconstruir A do zero, nao precisa do PG rodando
    local STATE_A
    STATE_A=$(get_pg_state_remote_a)
    if [ "$SPLITBRAIN_PROTECTED" = "1" ]; then
        log_ok "Pre-flight: A parado por split-brain — estado esperado para basebackup."
    elif [ "$STATE_A" = "down" ]; then
        log_error "Pre-flight: PostgreSQL em A nao responde"
        ERRORS=$((ERRORS + 1))
    else
        log_ok "Pre-flight: PostgreSQL em A OK (role=${STATE_A})"
    fi

    # 3. Espaco livre em A
    local FREE_A
    FREE_A=$(ssh_a "df --output=avail -m /var/lib/pgsql | tail -1" 2>/dev/null | tr -d ' ' || echo 0)
    if [ "${FREE_A:-0}" -lt "$DISK_MIN_FREE_MB" ]; then
        log_error "Pre-flight: Espaco em A insuficiente (${FREE_A}MB < ${DISK_MIN_FREE_MB}MB)"
        ERRORS=$((ERRORS + 1))
    else
        log_ok "Pre-flight: Espaco em A OK (${FREE_A}MB livres)"
    fi

    # 4. Espaco livre em B
    local FREE_B
    FREE_B=$(df --output=avail -m /var/lib/pgsql | tail -1 | tr -d ' ')
    if [ "${FREE_B:-0}" -lt "$DISK_MIN_FREE_MB" ]; then
        log_error "Pre-flight: Espaco em B insuficiente (${FREE_B}MB < ${DISK_MIN_FREE_MB}MB)"
        ERRORS=$((ERRORS + 1))
    else
        log_ok "Pre-flight: Espaco em B OK (${FREE_B}MB livres)"
    fi

    # 5. B esta como master
    if ! check_if_promoted; then
        log_error "Pre-flight: B nao esta como master"
        ERRORS=$((ERRORS + 1))
    fi

    # 6. Conectividade A -> B via TCP
    # Se SPLITBRAIN_PROTECTED=1, A esta parado — nao tem como testar conectividade
    # O pg_basebackup usa as credenciais do pgpass configurado no passo seguinte
    if [ "$SPLITBRAIN_PROTECTED" = "1" ]; then
        log_ok "Pre-flight: Conectividade A->B pulada (A parado por split-brain — sera testada pelo pg_basebackup)."
    elif ! ssh_a "sudo runuser -u postgres -- $PG_BIN/psql \
                -h $SLAVE_B_IP -U $REPL_USER -d $HEALTH_DB \
                -c 'SELECT 1' >/dev/null 2>&1"; then
        log_error "Pre-flight: A nao consegue conectar em B — pg_basebackup vai falhar"
        ERRORS=$((ERRORS + 1))
    else
        log_ok "Pre-flight: Conectividade A->B OK"
    fi

    # 7. Lag de replicacao de A
    # Se SPLITBRAIN_PROTECTED=1, A esta parado — lag nao e verificavel nem relevante
    # O basebackup vai copiar todos os dados de B para A do zero
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
    systemctl is-active --quiet postgresql-16 && return 0

    log_warn "PostgreSQL inativo. Tentando iniciar..."
    systemctl start postgresql-16 >> "$LOGFILE" 2>&1
    sleep 5

    if systemctl is-active --quiet postgresql-16; then
        log_ok "PostgreSQL iniciou com sucesso."
        return 0
    fi

    log_error "PostgreSQL falhou ao iniciar."
    local _DISK_B _MEM_B _LOAD_B
    _DISK_B=$(get_disk_info_local)
    _MEM_B=$(get_mem_info_local)
    _LOAD_B=$(get_load_local)
    send_telegram "🚨 *POSTGRESQL NÃO SUBIU*

🖥 *Host:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP})

📊 *Métricas no momento da falha:*
   💾 Disco B: ${_DISK_B}
   🧠 Memória B: ${_MEM_B}
   ⚡ Load B: ${_LOAD_B}

🔍 journalctl -u postgresql-16 -n 50
🕐 Horário: $(timestamp)"
    exit 1
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
    local STATE
    STATE=$(get_pg_state_local)
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
    local HOJE
    HOJE=$(date +%Y-%m-%d)
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
    _DISK_B=$(get_disk_info_local)
    _MEM_B=$(get_mem_info_local)
    _LOAD_B=$(get_load_local)
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

    systemctl stop postgresql-16 2>/dev/null || true
    sleep 3
    systemctl start postgresql-16 >> "$LOGFILE" 2>&1 || true
    sleep 5

    local STATE
    STATE=$(get_pg_state_local)
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
    _DISK_B=$(get_disk_info_local)
    _DISK_A=$(get_disk_info_remote)
    _MEM_B=$(get_mem_info_local)
    _LOAD_B=$(get_load_local)
    send_telegram "🔄 *FAILBACK INICIANDO*

🎯 *Objetivo:* Restaurar A como master

🖥 *Node A:* ${MASTER_HOSTNAME} (${MASTER_A_IP}) — voltando
🖥 *Node B:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP}) — master atual

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

    # ── [1/6] Parar PostgreSQL em A ──────────────────────────────────────
    log "  [1/6] Parando PostgreSQL em A..."
    if ! ssh_a_retry "sudo systemctl stop postgresql-16"; then
        log_error "Nao foi possivel parar PostgreSQL em A"
        rollback_failback; return 1
    fi
    log_ok "  [1/6] PostgreSQL em A parado."

    # ── [2/6] Limpar PGDATA em A ──────────────────────────────────────────
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

    # ── [3/6] pg_basebackup B → A ─────────────────────────────────────────
    # tee fora do ssh_a — grava progresso no log do node B (local)
    log "  [3/6] pg_basebackup B -> A (timeout: ${BASEBACKUP_TIMEOUT}s)..."
    if ! ssh_a "sudo runuser -u postgres -- timeout $BASEBACKUP_TIMEOUT \
                $PG_BIN/pg_basebackup \
                -h $SLAVE_B_IP -D $PGDATA -U $REPL_USER \
                -P -v -R --wal-method=stream --slot=$REMOTE_SLOT" \
                2>&1 | tee -a "$LOGFILE" ; then
        log_error "pg_basebackup B->A falhou"
        rollback_failback; return 1
    fi
    log_ok "  [3/6] pg_basebackup B->A concluido."

    # ── [4/6] Promover A como master ──────────────────────────────────────
    log "  [4/6] Iniciando PostgreSQL em A e promovendo..."
    if ! ssh_a "sudo systemctl start postgresql-16"; then
        log_error "Nao foi possivel iniciar PostgreSQL em A"
        rollback_failback; return 1
    fi
    sleep 5

    if ! ssh_a "sudo runuser -u postgres -- $PG_BIN/pg_ctl -D $PGDATA promote"; then
        log_error "Promocao de A falhou"
        rollback_failback; return 1
    fi

    # Aguardar A sair do recovery com retry
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

    # Subir servicos em A (check_system) — igual ao que e feito no failover de B
    log_info "  [4/6] Executando check_system em A para subir servicos..."
    if ssh_a "sudo /usr/local/rbenv/shims/ruby /home/totalip/ipserver/nagios/check_system.rb -v >> /var/log/failover.log 2>&1"; then
        log_ok "  [4/6] check_system em A executado com sucesso."
    else
        log_warn "  [4/6] check_system em A retornou erro (nao critico — continuando)."
    fi

    ensure_replication_slot_remote_a "$MY_SLOT"
    sleep 10

    # ── [5/6] Reconstruir B como slave ────────────────────────────────────
    log "  [5/6] Reconstruindo B como slave..."
    check_asterisk
    systemctl stop postgresql-16

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

    # ── [6/6] Subir B como slave ───────────────────────────────────────────
    log "  [6/6] Iniciando PostgreSQL em B como slave..."
    systemctl start postgresql-16
    sleep 5

    local B_STATE
    B_STATE=$(get_pg_state_local)
    if [ "$B_STATE" != "replica" ]; then
        log_error "B nao iniciou como slave apos failback (state=$B_STATE)"
        rollback_failback; return 1
    fi
    log_ok "  [6/6] B iniciado como slave."

    pkill_if_recovery
    MASTER_HEALTH_COUNT=0
    SPLITBRAIN_PROTECTED=0
    echo "$(timestamp)" > "$FLAG_FAILBACK"
    log_ok "FAILBACK concluido com sucesso"

    local _DUR_FB _DISK_A _DISK_B _MEM_A _LOAD_A
    _DUR_FB=$(calc_duration "$FAILBACK_START_TS")
    _DISK_A=$(get_disk_info_remote)
    _DISK_B=$(get_disk_info_local)
    _MEM_A=$(get_mem_info_remote)
    _LOAD_A=$(get_load_remote)
    send_telegram "✅ *FAILBACK CONCLUÍDO*

👑 *Master restaurado:* ${MASTER_HOSTNAME} (${MASTER_A_IP})
🔄 *Replica:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP})

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
#
# Melhoria 2: verifica lag antes de promover
# Se a replica estiver muito atrasada, avisa mas pode prosseguir
# dependendo de MAX_LAG_FOR_FAILOVER
########################################
do_failover() {
    log "Promovendo B (${SLAVE_HOSTNAME}) para master..."
    ensure_postgres_running

    # Verificar lag antes de promover
    if ! check_local_lag_acceptable; then
        log_warn "B com lag alto — promovendo mesmo assim (A confirmado offline)"
        log_warn "ATENCAO: pode haver perda de dados!"
        local _LAG_B
        _LAG_B=$(get_local_wal_lag_bytes 2>/dev/null || echo "N/A")
        send_telegram "⚠️ *FAILOVER COM LAG ALTO*

🖥 *Node B:* ${SLAVE_HOSTNAME}
📡 *Lag WAL de B:* ${_LAG_B} bytes (limite: ${MAX_LAG_FOR_FAILOVER})

⚠️ A está offline e não há alternativa — promovendo B mesmo assim
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

    local FINAL_STATE
    FINAL_STATE=$(get_pg_state_local)
    if [ "$FINAL_STATE" != "master" ]; then
        log_error "B nao assumiu como master apos promocao (state=$FINAL_STATE)"
        local _DISK_B _MEM_B _LOAD_B
        _DISK_B=$(get_disk_info_local)
        _MEM_B=$(get_mem_info_local)
        _LOAD_B=$(get_load_local)
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
    /usr/local/rbenv/shims/ruby \
        /home/totalip/ipserver/nagios/check_system.rb -v \
        >> "$LOGFILE" 2>&1 || log_warn "check_system.rb retornou erro"

    local _DUR_FO _DISK_B _MEM_B _LOAD_B _LAG
    _DUR_FO=$(calc_duration "$FAILOVER_START_TS")
    _DISK_B=$(get_disk_info_local)
    _MEM_B=$(get_mem_info_local)
    _LOAD_B=$(get_load_local)
    _LAG=$(get_local_wal_lag_bytes 2>/dev/null || echo "N/A")
    send_telegram "✅ *FAILOVER CONCLUÍDO*

🖥 *Novo Master:* ${SLAVE_HOSTNAME} (${SLAVE_B_IP})
🔌 *Master anterior:* ${MASTER_HOSTNAME} (${MASTER_A_IP}) — OFFLINE

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
log "  No local  : ${SLAVE_HOSTNAME} (${SLAVE_B_IP})"
log "  No remoto : ${MASTER_A_IP}"
log "  Failover threshold : ${FAILOVER_THRESHOLD} falhas consecutivas (ping+ssh+pg)"
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
    MASTER_HOSTNAME=$(get_master_hostname)

    # ------------------------------------------------------------------ #
    # MASTER A ONLINE
    # ------------------------------------------------------------------ #
    if check_master_online; then
        FAILURE_COUNT=0
        LOCAL_STATE=$(get_pg_state_local)

        # Limpar flags quando A voltou — DEVE VIR ANTES de qualquer checagem
        # O continue abaixo pularia este bloco se ficasse depois
        if [ -s "$FLAG_FAILOVER" ]; then
            log_info "A (${MASTER_HOSTNAME}) voltou online. Reiniciando ciclo de failback."
            local _DISK_A _MEM_A _LOAD_A _STATE_A
            _DISK_A=$(get_disk_info_remote)
            _MEM_A=$(get_mem_info_remote)
            _LOAD_A=$(get_load_remote)
            _STATE_A=$(get_pg_state_remote_a)
            send_telegram "🟢 *MASTER A VOLTOU ONLINE*

🖥 *Node A:* ${MASTER_HOSTNAME} (${MASTER_A_IP})
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

        # B e master — avaliar failback
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

        # B e replica — estado normal
        if [ "$LOCAL_STATE" = "replica" ]; then
            log_info "B=replica, A=online — estado normal."
        fi

    # ------------------------------------------------------------------ #
    # MASTER A OFFLINE — aguarda confirmação tripla antes de failover
    # ------------------------------------------------------------------ #
    else
        MASTER_HEALTH_COUNT=0

        if master_confirmed_offline; then

            # FIX: A caiu de novo apos failback ja concluido
            # Limpar FLAG_FAILBACK para permitir novo ciclo quando A voltar
            if [ -s "$FLAG_FAILBACK" ]; then
                log_info "A caiu novamente apos failback anterior. Reiniciando ciclo."
                > "$FLAG_FAILBACK"
            fi

            # FIX: resetar contador apos atingir threshold (evita log poluido)
            if [ "$FAILURE_COUNT" -gt "$FAILOVER_THRESHOLD" ]; then
                FAILURE_COUNT=$FAILOVER_THRESHOLD
            fi

            if [ ! -s "$FLAG_FAILOVER" ]; then
                FAILOVER_START_TS=$(date +%s)
                local _DISK_B _MEM_B _LOAD_B
                _DISK_B=$(get_disk_info_local)
                _MEM_B=$(get_mem_info_local)
                _LOAD_B=$(get_load_local)
                send_telegram "🔴 *FAILOVER INICIADO*

🖥 *Master A:* ${MASTER_HOSTNAME} (${MASTER_A_IP})
   Status: OFFLINE confirmado (${FAILOVER_THRESHOLD}/${FAILOVER_THRESHOLD} checks)

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

    # Detectar mudanca de estado local
    CURRENT_STATE=$(get_pg_state_local)
    if [ "$CURRENT_STATE" != "$LAST_STATE" ]; then
        log "Mudanca de estado em B: ${LAST_STATE} -> ${CURRENT_STATE}"
        pkill_if_recovery
        LAST_STATE="$CURRENT_STATE"
    fi

    sleep "$CHECK_INTERVAL"
done
