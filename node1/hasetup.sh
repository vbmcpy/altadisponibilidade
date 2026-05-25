#!/usr/bin/env bash
# =============================================================================
# hasetup.sh
# Configura privilegios minimos do usuario rsync para o pg-failover.sh
#
# Uso:
#   chmod +x hasetup.sh
#   sudo ./hasetup.sh
#
# Log persistente em /var/log/hasetup.log (com timestamp em cada linha,
# cores apenas na tela). Mesmo em caso de erro precoce o log e gravado.
# =============================================================================
set -euo pipefail

# PATH garantido — sbin/usbin precisam estar disponiveis para visudo, usermod etc.
# Em algumas invocacoes (cron, su, sudo com secure_path restrito) /usr/sbin nao
# aparece no PATH herdado, e visudo falha com "command not found"
export PATH="/usr/sbin:/sbin:/usr/bin:/bin${PATH:+:$PATH}"

# ─────────────────────────────────────────────────────────────────────────────
# LOG SETUP — PRIMEIRA COISA, antes de qualquer operacao
# ─────────────────────────────────────────────────────────────────────────────
LOG_FILE="/var/log/hasetup.log"

# Fallback: se nao tiver permissao em /var/log (ex.: rodando sem sudo),
# escreve em /tmp para que o erro "Execute como root" tambem fique gravado.
if ! ( : >> "$LOG_FILE" ) 2>/dev/null; then
    LOG_FILE="/tmp/hasetup-$(id -un).log"
fi
: >> "$LOG_FILE" || { echo "FATAL: nao foi possivel criar $LOG_FILE"; exit 2; }

# fd3 = stdout original do terminal (mantem cores la)
exec 3>&1
# stdout/stderr passam por um pipe que: imprime colorido na tela (fd3)
# e grava sem cor + com timestamp no arquivo de log.
# set +e + trap PIPE + || true = subshell resiliente, nunca trunca log
exec > >(
    set +e
    trap '' PIPE
    while IFS= read -r line; do
        ts="$(date '+%F %T')"
        plain="$(printf '%s' "$line" | sed -r 's/\x1b\[[0-9;]*m//g' 2>/dev/null)"
        printf '%s\n' "$line" >&3 2>/dev/null || true
        printf '[%s] %s\n' "$ts" "$plain" >> "$LOG_FILE" 2>/dev/null || true
    done
)
exec 2>&1

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
RED=$'\033[0;31m'; GREEN=$'\033[0;32m'; YELLOW=$'\033[1;33m'
BLUE=$'\033[0;34m'; CYAN=$'\033[0;36m'; NC=$'\033[0m'

CHANGES=0
SKIPS=0

ok()      { echo "${GREEN}[OK]${NC}       $*"; SKIPS=$((SKIPS+1)); }
change()  { echo "${YELLOW}[CHANGED]${NC}  $*"; CHANGES=$((CHANGES+1)); }
info()    { echo "${BLUE}[INFO]${NC}     $*"; }
warn()    { echo "${YELLOW}[WARN]${NC}     $*"; }
verify()  { echo "${CYAN}[VERIFY]${NC}   $*"; }
fail()    { echo "${RED}[ERRO]${NC}     $*"; echo "${RED}[FATAL]${NC}    hasetup.sh ABORTADO"; exit 1; }
section() {
    echo ""
    echo "─────────────────────────────────────────────────────────────"
    echo "  $*"
    echo "─────────────────────────────────────────────────────────────"
}

# Flush garantido do pipe de log quando o script termina
cleanup() {
    local rc=$?
    sleep 0.2
    exec 1>&3 2>&3 || true
    if [[ $rc -ne 0 ]]; then
        echo "[$(date '+%F %T')] [EXIT] hasetup.sh saiu com rc=$rc" >> "$LOG_FILE"
    fi
}
trap cleanup EXIT

# ─────────────────────────────────────────────────────────────────────────────
# CABECALHO
# ─────────────────────────────────────────────────────────────────────────────
echo ""
echo "============================================================="
echo "  hasetup.sh — INICIO"
echo "============================================================="
echo "  Data    : $(date '+%F %T %Z')"
echo "  Host    : $(hostname)"
echo "  Usuario : $(id -un) (UID=$EUID)"
echo "  PID     : $$"
echo "  Args    : $*"
echo "  Log     : $LOG_FILE"
echo "============================================================="

# ─────────────────────────────────────────────────────────────────────────────
# 1/9 — ROOT CHECK
# ─────────────────────────────────────────────────────────────────────────────
section "1/9 — Verificando privilegios"
[[ $EUID -ne 0 ]] && fail "Execute como root: sudo $0"
ok "Executando como root"

# ─────────────────────────────────────────────────────────────────────────────
# 2/9 — DETECTAR POSTGRESQL
# ─────────────────────────────────────────────────────────────────────────────
section "2/9 — Detectando PostgreSQL"
PG_VER=""
for v in 16 15 14 13; do
    if [[ -x "/usr/pgsql-${v}/bin/pg_ctl" ]]; then
        PG_VER="$v"; break
    fi
done
[[ -z "$PG_VER" ]] && fail "PostgreSQL nao encontrado em /usr/pgsql-*/bin/"
PG_BIN="/usr/pgsql-${PG_VER}/bin"
PGDATA="/var/lib/pgsql/${PG_VER}/data"
ok "PostgreSQL ${PG_VER} detectado em ${PG_BIN}"
if [[ -d "$PGDATA" ]]; then
    info "PGDATA: ${PGDATA} (existe)"
else
    info "PGDATA: ${PGDATA} (ainda nao existe — normal em standby novo)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 3/9 — USUARIO RSYNC
# ─────────────────────────────────────────────────────────────────────────────
section "3/9 — Verificando usuario rsync"
id rsync &>/dev/null || fail "Usuario rsync nao existe"
RSYNC_UID=$(id -u rsync)
ok "Usuario rsync existe (uid=${RSYNC_UID})"

# ─────────────────────────────────────────────────────────────────────────────
# 4/9 — postgres NO GRUPO totalip
# ─────────────────────────────────────────────────────────────────────────────
section "4/9 — Garantindo postgres no grupo totalip"
if id postgres | grep -qE '(\(|,)totalip(\)|,)'; then
    ok "postgres ja e membro do grupo totalip"
else
    usermod -aG totalip postgres
    change "postgres adicionado ao grupo totalip"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 5/9 — DETECTAR BINARIOS
# ─────────────────────────────────────────────────────────────────────────────
section "5/9 — Detectando binarios necessarios"

# timeout
if [[ -x "/bin/timeout" ]]; then
    TIMEOUT_BIN="/bin/timeout"
elif [[ -x "/usr/bin/timeout" ]]; then
    TIMEOUT_BIN="/usr/bin/timeout"
else
    TIMEOUT_BIN=$(which timeout 2>/dev/null || true)
fi
[[ -z "$TIMEOUT_BIN" ]] && fail "timeout nao encontrado"

# runuser
RUNUSER_BIN=$(which runuser 2>/dev/null || echo "/sbin/runuser")

# rsync
RSYNC_BIN=""
for p in /usr/bin/rsync /bin/rsync /usr/local/bin/rsync; do
    if [[ -x "$p" ]]; then RSYNC_BIN="$p"; break; fi
done
if [[ -z "$RSYNC_BIN" ]]; then
    RSYNC_BIN=$(which rsync 2>/dev/null || true)
fi
[[ -z "$RSYNC_BIN" ]] && fail "rsync nao encontrado"

# visudo — caminho absoluto para nao depender de PATH na validacao
VISUDO_BIN=""
for p in /usr/sbin/visudo /sbin/visudo /usr/bin/visudo; do
    if [[ -x "$p" ]]; then VISUDO_BIN="$p"; break; fi
done
if [[ -z "$VISUDO_BIN" ]]; then
    VISUDO_BIN=$(command -v visudo 2>/dev/null || true)
fi
[[ -z "$VISUDO_BIN" ]] && fail "visudo nao encontrado em /usr/sbin, /sbin nem /usr/bin"

ok "rsync=${RSYNC_BIN} | visudo=${VISUDO_BIN}"
ok "timeout=${TIMEOUT_BIN} | runuser=${RUNUSER_BIN}"

# ─────────────────────────────────────────────────────────────────────────────
# 6/9 — /etc/sudoers PRINCIPAL
# ─────────────────────────────────────────────────────────────────────────────
section "6/9 — Configurando /etc/sudoers principal"

if grep -q "^#includedir /etc/sudoers.d" /etc/sudoers; then
    ok "#includedir /etc/sudoers.d ja presente"
else
    echo "#includedir /etc/sudoers.d" >> /etc/sudoers
    change "#includedir /etc/sudoers.d adicionado ao /etc/sudoers"
fi

if grep -q "^Defaults.*requiretty" /etc/sudoers; then
    if grep -q "^Defaults:rsync.*!requiretty" /etc/sudoers; then
        ok "Defaults:rsync !requiretty ja configurado"
    else
        REQTTY_LINE=$(grep -n "^Defaults.*requiretty" /etc/sudoers | head -1 | cut -d: -f1)
        sed -i "${REQTTY_LINE}a Defaults:rsync    !requiretty" /etc/sudoers
        change "Defaults:rsync !requiretty adicionado em /etc/sudoers linha ${REQTTY_LINE}"
    fi
else
    ok "requiretty nao esta global — nada a fazer"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 7/9 — LIMPAR SUDOERS ANTIGO
# ─────────────────────────────────────────────────────────────────────────────
section "7/9 — Removendo sudoers antigo (se existir)"
if [[ -f /etc/sudoers.d/rsync ]]; then
    rm -f /etc/sudoers.d/rsync
    change "/etc/sudoers.d/rsync (permissoes amplas) removido"
else
    ok "/etc/sudoers.d/rsync nao existe (nada a remover)"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 8/9 — CRIAR SUDOERS MINIMO
# ─────────────────────────────────────────────────────────────────────────────
section "8/9 — Gerando /etc/sudoers.d/pg-failover-remote"
SUDOERS_FILE="/etc/sudoers.d/pg-failover-remote"

OLD_HASH=""
BACKUP_FILE=""
if [[ -f "$SUDOERS_FILE" ]]; then
    OLD_HASH=$(md5sum "$SUDOERS_FILE" | awk '{print $1}')
    BACKUP_FILE="${SUDOERS_FILE}.bak.$$"
    cp -p "$SUDOERS_FILE" "$BACKUP_FILE"
    info "Backup criado: $BACKUP_FILE"
fi

cat > "$SUDOERS_FILE" << EOF
# pg-failover-remote — Gerado por hasetup.sh em $(date '+%F %T')
# Privilegios minimos para rsync executar comandos via SSH
# PostgreSQL ${PG_VER} | PGDATA: ${PGDATA}

Defaults:rsync !requiretty

# ── rsync binario ──────────────────────────────────────────────────────
rsync ALL=(root) NOPASSWD: ${RSYNC_BIN} *

# ── PostgreSQL service ─────────────────────────────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/systemctl stop postgresql-${PG_VER}
rsync ALL=(root) NOPASSWD: /usr/bin/systemctl start postgresql-${PG_VER}
rsync ALL=(root) NOPASSWD: /bin/systemctl stop postgresql-${PG_VER}
rsync ALL=(root) NOPASSWD: /bin/systemctl start postgresql-${PG_VER}

# ── runuser psql / pg_ctl ──────────────────────────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/runuser -u postgres -- ${PG_BIN}/psql *
rsync ALL=(root) NOPASSWD: /usr/bin/runuser -u postgres -- ${PG_BIN}/pg_ctl *
rsync ALL=(root) NOPASSWD: /sbin/runuser -u postgres -- ${PG_BIN}/psql *
rsync ALL=(root) NOPASSWD: /sbin/runuser -u postgres -- ${PG_BIN}/pg_ctl *

# ── pg_basebackup com timeout ──────────────────────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/runuser -u postgres -- ${TIMEOUT_BIN} * ${PG_BIN}/pg_basebackup *
rsync ALL=(root) NOPASSWD: /sbin/runuser -u postgres -- ${TIMEOUT_BIN} * ${PG_BIN}/pg_basebackup *
rsync ALL=(root) NOPASSWD: /usr/sbin/runuser -u postgres -- ${TIMEOUT_BIN} * ${PG_BIN}/pg_basebackup *

# ── PGDATA restrito ────────────────────────────────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/rm -rf ${PGDATA}
rsync ALL=(root) NOPASSWD: /bin/rm -rf ${PGDATA}
rsync ALL=(root) NOPASSWD: /usr/bin/mkdir -p ${PGDATA}
rsync ALL=(root) NOPASSWD: /bin/mkdir -p ${PGDATA}
rsync ALL=(root) NOPASSWD: /usr/bin/chown postgres\:postgres ${PGDATA}
rsync ALL=(root) NOPASSWD: /bin/chown postgres\:postgres ${PGDATA}
rsync ALL=(root) NOPASSWD: /usr/bin/chmod 700 ${PGDATA}
rsync ALL=(root) NOPASSWD: /bin/chmod 700 ${PGDATA}

# ── mkdir dirs de sync ─────────────────────────────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/mkdir -p /home/totalip
rsync ALL=(root) NOPASSWD: /usr/bin/mkdir -p /etc/asterisk
rsync ALL=(root) NOPASSWD: /usr/bin/mkdir -p /var/lib/asterisk/sounds
rsync ALL=(root) NOPASSWD: /bin/mkdir -p /home/totalip
rsync ALL=(root) NOPASSWD: /bin/mkdir -p /etc/asterisk
rsync ALL=(root) NOPASSWD: /bin/mkdir -p /var/lib/asterisk/sounds

# ── find PGDATA ────────────────────────────────────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/find ${PGDATA} *
rsync ALL=(root) NOPASSWD: /bin/find ${PGDATA} *

# ── check_system pos-failback ──────────────────────────────────────────
rsync ALL=(root) NOPASSWD: /bin/bash -c /usr/local/rbenv/shims/ruby /home/totalip/ipserver/nagios/check_system.rb -v >> /var/log/failover.log 2>&1

# ── pkill check_system ─────────────────────────────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/pkill -f check_system.rb
rsync ALL=(root) NOPASSWD: /bin/pkill -f check_system.rb
EOF

chmod 440 "$SUDOERS_FILE"
NEW_HASH=$(md5sum "$SUDOERS_FILE" | awk '{print $1}')

if [[ "$OLD_HASH" == "$NEW_HASH" ]]; then
    ok "$SUDOERS_FILE inalterado (md5=${NEW_HASH:0:12}...)"
elif [[ -z "$OLD_HASH" ]]; then
    change "$SUDOERS_FILE criado (md5=${NEW_HASH:0:12}...)"
else
    change "$SUDOERS_FILE atualizado (md5: ${OLD_HASH:0:12}... -> ${NEW_HASH:0:12}...)"
fi

FILE_SIZE=$(stat -c '%s' "$SUDOERS_FILE")
FILE_LINES=$(wc -l < "$SUDOERS_FILE")
FILE_PERMS=$(stat -c '%a' "$SUDOERS_FILE")
RULE_COUNT=$(grep -cE "^rsync\s+ALL=" "$SUDOERS_FILE")
info "Arquivo: ${FILE_SIZE} bytes, ${FILE_LINES} linhas, perms ${FILE_PERMS}, ${RULE_COUNT} regras NOPASSWD"

echo ""
echo "  Permissoes concedidas ao usuario rsync (NOPASSWD via sudo):"
echo ""
echo "  • Sincronizar arquivos via SSH"
echo "      ${RSYNC_BIN} *"
echo ""
echo "  • Parar/iniciar o servico postgresql-${PG_VER} (failover)"
echo "      systemctl stop/start postgresql-${PG_VER}"
echo ""
echo "  • Executar comandos PostgreSQL como usuario postgres"
echo "      runuser -u postgres -- psql      (queries de orquestracao)"
echo "      runuser -u postgres -- pg_ctl    (controle do cluster)"
echo "      runuser -u postgres -- timeout * pg_basebackup    (clonar master)"
echo ""
echo "  • Recriar o PGDATA (${PGDATA})"
echo "      rm -rf, mkdir -p, chown postgres:postgres, chmod 700"
echo ""
echo "  • Criar diretorios de sincronizacao"
echo "      mkdir -p /home/totalip /etc/asterisk /var/lib/asterisk/sounds"
echo ""
echo "  • Operacoes auxiliares"
echo "      find ${PGDATA} (verificar conteudo)"
echo "      pkill -f check_system.rb (encerrar instancia anterior)"
echo "      bash -c 'ruby check_system.rb -v >> /var/log/failover.log 2>&1'"
echo ""

# ─── Validacao via visudo -c ─────────────────────────────────────────────
# Usa visudo -c (global) ja que ele inclui nosso arquivo via #includedir.
# Em sucesso: so o resumo. Em falha: dump completo + rollback.
VISUDO_OUT=""
VISUDO_RC=0
VISUDO_OUT=$("$VISUDO_BIN" -c 2>&1) || VISUDO_RC=$?

if [[ $VISUDO_RC -eq 0 ]]; then
    FILES_VALIDATED=$(echo "$VISUDO_OUT" | grep -cE ":.*OK$" || true)
    ok "visudo -c: ${FILES_VALIDATED} arquivo(s) sudoers validados, sintaxe OK"
    # Sucesso — remove backup
    if [[ -n "$BACKUP_FILE" && -f "$BACKUP_FILE" ]]; then
        rm -f "$BACKUP_FILE"
        info "Backup $(basename $BACKUP_FILE) removido (validacao OK)"
    fi
else
    warn "visudo retornou rc=$VISUDO_RC. Saida completa:"
    echo "$VISUDO_OUT" | sed 's/^/                /'
    # Falha — restaura backup ou remove arquivo novo
    if [[ -n "$BACKUP_FILE" && -f "$BACKUP_FILE" ]]; then
        mv -f "$BACKUP_FILE" "$SUDOERS_FILE"
        warn "Backup restaurado: versao anterior do sudoers reativada"
    else
        rm -f "$SUDOERS_FILE"
        warn "Arquivo $SUDOERS_FILE removido (nao havia backup)"
    fi
    fail "Erro de sintaxe no sudoers — veja saida do visudo acima"
fi

# ─────────────────────────────────────────────────────────────────────────────
# 9/9 — LOG DE FAILOVER
# ─────────────────────────────────────────────────────────────────────────────
section "9/9 — Configurando /var/log/failover.log"
if [[ -f /var/log/failover.log ]]; then
    ok "/var/log/failover.log ja existe"
else
    touch /var/log/failover.log
    change "/var/log/failover.log criado"
fi

CUR_PERM=$(stat -c '%a' /var/log/failover.log)
if [[ "$CUR_PERM" != "666" ]]; then
    chmod 666 /var/log/failover.log
    change "/var/log/failover.log: permissao alterada de $CUR_PERM para 666"
else
    ok "/var/log/failover.log permissoes ja 666"
fi

# ─────────────────────────────────────────────────────────────────────────────
# VERIFICACAO FINAL
# Validacao baseada em arquivos (nao depende de TTY).
# `sudo -l -U rsync` e best-effort: se houver TTY, mostra o numero efetivo;
# se nao houver (cron, pipe), pulamos sem falsificar erro — o sistema tem
# Defaults requiretty global e isso bloqueia sudo de qualquer usuario sem TTY,
# inclusive root chamando outro sudo de dentro deste script.
# ─────────────────────────────────────────────────────────────────────────────
section "VERIFICACAO FINAL"

# 1) Quantas regras nosso arquivo declara (sempre confiavel)
RULES_IN_FILE=$(grep -cE "^rsync[[:space:]]+ALL=" "$SUDOERS_FILE" || true)
RULES_IN_FILE=${RULES_IN_FILE:-0}
ok "$SUDOERS_FILE declara ${RULES_IN_FILE} regras NOPASSWD para rsync"

# 2) Procura conflitos: outros arquivos em /etc/sudoers* com regras para rsync
CONFLICTS=$(grep -lE "^rsync[[:space:]]+ALL=" /etc/sudoers /etc/sudoers.d/* 2>/dev/null \
            | grep -vF "$SUDOERS_FILE" || true)
if [[ -n "$CONFLICTS" ]]; then
    warn "Outros arquivos sudoers tambem definem regras para rsync (potencial conflito):"
    echo "$CONFLICTS" | sed 's/^/                /'
else
    ok "Sem conflitos: somente $SUDOERS_FILE define regras para rsync"
fi

# 3) Tentativa opcional via sudo -l (precisa TTY por causa de Defaults requiretty)
RULES_OUT=$(sudo -n -l -U rsync 2>&1 || true)
if echo "$RULES_OUT" | grep -qiE "must have a tty|sorry,?[[:space:]]*you"; then
    info "sudo -l -U rsync precisa de TTY (Defaults requiretty global ativo)"
    info "Para validar manualmente, de um terminal interativo: sudo -l -U rsync"
elif echo "$RULES_OUT" | grep -qE '\(root\)[[:space:]]+NOPASSWD'; then
    ACTIVE_RULES=$(echo "$RULES_OUT" | grep -cE '\(root\)[[:space:]]+NOPASSWD' || true)
    ACTIVE_RULES=${ACTIVE_RULES:-0}
    if (( ACTIVE_RULES >= RULES_IN_FILE )); then
        ok "sudo confirma ${ACTIVE_RULES} regras NOPASSWD ativas (esperadas: ${RULES_IN_FILE})"
    else
        warn "sudo entrega ${ACTIVE_RULES}/${RULES_IN_FILE} regras — possivel sombreamento"
    fi
else
    warn "sudo -l retornou saida inesperada (primeiras linhas):"
    echo "$RULES_OUT" | head -5 | sed 's/^/                /'
fi

# ─────────────────────────────────────────────────────────────────────────────
# RESUMO
# ─────────────────────────────────────────────────────────────────────────────
section "RESUMO"
echo ""
echo "  Arquivo sudoers : $SUDOERS_FILE"
echo "  PostgreSQL Ver  : ${PG_VER}"
echo "  PG Bin          : ${PG_BIN}"
echo "  PGDATA          : ${PGDATA}"
echo "  rsync           : ${RSYNC_BIN}"
echo "  timeout         : ${TIMEOUT_BIN}"
echo "  runuser         : ${RUNUSER_BIN}"
echo ""
echo "  Alteracoes neste run : $CHANGES"
echo "  Itens ja corretos    : $SKIPS"
echo ""
echo "============================================================="
if [[ $CHANGES -gt 0 ]]; then
    echo "  ${GREEN}hasetup.sh CONCLUIDO — $CHANGES alteracao(oes) aplicada(s)${NC}"
else
    echo "  ${GREEN}hasetup.sh CONCLUIDO — sistema ja estava configurado${NC}"
fi
echo "  Log completo: $LOG_FILE"
echo "============================================================="
echo ""
