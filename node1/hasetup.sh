#!/usr/bin/env bash
# =============================================================================
# hasetup.sh
# Configura privilégios mínimos do usuário rsync para o pg-failover.sh
#
#
#   chmod +x hasetup.sh
#   ./hasetup.sh
# =============================================================================
set -euo pipefail

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}[OK]${NC}    $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC}  $*"; }
fail() { echo -e "${RED}[ERRO]${NC}  $*"; exit 1; }

[[ $EUID -ne 0 ]] && fail "Execute como root: sudo $0"

# ─────────────────────────────────────────────
# DETECTAR POSTGRESQL
# ─────────────────────────────────────────────
PG_VER=""
for v in 16 15 14 13; do
    if [[ -x "/usr/pgsql-${v}/bin/pg_ctl" ]]; then
        PG_VER="$v"; break
    fi
done
[[ -z "$PG_VER" ]] && fail "PostgreSQL não encontrado em /usr/pgsql-*/bin/"

PG_BIN="/usr/pgsql-${PG_VER}/bin"
PGDATA="/var/lib/pgsql/${PG_VER}/data"
ok "PostgreSQL ${PG_VER} — ${PG_BIN}"

# ─────────────────────────────────────────────
# VERIFICAR USUARIO RSYNC
# ─────────────────────────────────────────────
id rsync &>/dev/null || fail "Usuário rsync não existe"
ok "Usuário rsync: existe"

# ─────────────────────────────────────────────
# GARANTIR postgres NO GRUPO totalip
# psql tem permissão rwxr-x--- totalip — postgres precisa estar no grupo
# ─────────────────────────────────────────────
if id postgres | grep -q totalip; then
    ok "postgres já é membro do grupo totalip"
else
    warn "Adicionando postgres ao grupo totalip..."
    usermod -aG totalip postgres
    ok "postgres adicionado ao grupo totalip"
fi

# ─────────────────────────────────────────────
# DETECTAR TIMEOUT E RUNUSER
# ─────────────────────────────────────────────
# Detectar timeout — preferir /bin/timeout se existir
if [[ -x "/bin/timeout" ]]; then
    TIMEOUT_BIN="/bin/timeout"
elif [[ -x "/usr/bin/timeout" ]]; then
    TIMEOUT_BIN="/usr/bin/timeout"
else
    TIMEOUT_BIN=$(which timeout 2>/dev/null || fail "timeout não encontrado")
fi
RUNUSER_BIN=$(which runuser 2>/dev/null || echo "/sbin/runuser")
ok "timeout  : ${TIMEOUT_BIN}"
ok "runuser  : ${RUNUSER_BIN}"

# ─────────────────────────────────────────────
# GARANTIR #includedir NO SUDOERS PRINCIPAL
# ─────────────────────────────────────────────
if ! grep -q "^#includedir /etc/sudoers.d" /etc/sudoers; then
    echo "#includedir /etc/sudoers.d" >> /etc/sudoers
    ok "#includedir /etc/sudoers.d adicionado ao sudoers principal"
else
    ok "#includedir já presente no sudoers principal"
fi

# ─────────────────────────────────────────────
# GARANTIR !requiretty PARA rsync NO SUDOERS PRINCIPAL
# O Defaults requiretty global bloqueia SSH não-interativo
# ─────────────────────────────────────────────
if grep -q "^Defaults.*requiretty" /etc/sudoers && ! grep -q "^Defaults:rsync.*!requiretty" /etc/sudoers; then
    REQTTY_LINE=$(grep -n "^Defaults.*requiretty" /etc/sudoers | head -1 | cut -d: -f1)
    sed -i "${REQTTY_LINE}a Defaults:rsync    !requiretty" /etc/sudoers
    ok "Defaults:rsync !requiretty adicionado ao sudoers principal (linha ${REQTTY_LINE})"
else
    ok "!requiretty para rsync já configurado"
fi

# ─────────────────────────────────────────────
# REMOVER SUDOERS ANTIGO COM PERMISSOES AMPLAS
# ─────────────────────────────────────────────
if [[ -f /etc/sudoers.d/rsync ]]; then
    warn "Removendo /etc/sudoers.d/rsync (permissões amplas)..."
    rm -f /etc/sudoers.d/rsync
    ok "Sudoers antigo removido"
fi

# ─────────────────────────────────────────────
# CRIAR SUDOERS MINIMO E DEFINITIVO
# ─────────────────────────────────────────────
SUDOERS_FILE="/etc/sudoers.d/pg-failover-remote"

cat > "$SUDOERS_FILE" << EOF
# pg-failover-remote — Gerado por setup-sudoers-pg-failover.sh
# Privilégios mínimos para rsync executar comandos do pg-failover.sh via SSH
# PostgreSQL ${PG_VER} | PGDATA: ${PGDATA} | $(date '+%Y-%m-%d')

Defaults:rsync !requiretty

# ── PostgreSQL service ──────────────────────────────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/systemctl stop postgresql-${PG_VER}
rsync ALL=(root) NOPASSWD: /usr/bin/systemctl start postgresql-${PG_VER}
rsync ALL=(root) NOPASSWD: /bin/systemctl stop postgresql-${PG_VER}
rsync ALL=(root) NOPASSWD: /bin/systemctl start postgresql-${PG_VER}

# ── runuser: psql e pg_ctl como postgres ────────────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/runuser -u postgres -- ${PG_BIN}/psql *
rsync ALL=(root) NOPASSWD: /usr/bin/runuser -u postgres -- ${PG_BIN}/pg_ctl *
rsync ALL=(root) NOPASSWD: /sbin/runuser -u postgres -- ${PG_BIN}/psql *
rsync ALL=(root) NOPASSWD: /sbin/runuser -u postgres -- ${PG_BIN}/pg_ctl *

# ── pg_basebackup com timeout — caminho completo obrigatório ────────────
# Ambos /usr/bin/runuser e /sbin/runuser incluídos (varia por distro/versão)
rsync ALL=(root) NOPASSWD: /usr/bin/runuser -u postgres -- ${TIMEOUT_BIN} * ${PG_BIN}/pg_basebackup *
rsync ALL=(root) NOPASSWD: /sbin/runuser -u postgres -- ${TIMEOUT_BIN} * ${PG_BIN}/pg_basebackup *
rsync ALL=(root) NOPASSWD: /usr/sbin/runuser -u postgres -- ${TIMEOUT_BIN} * ${PG_BIN}/pg_basebackup *

# ── PGDATA — restrito ao caminho exato ${PGDATA} ────────────────────────
rsync ALL=(root) NOPASSWD: /usr/bin/rm -rf ${PGDATA}
rsync ALL=(root) NOPASSWD: /bin/rm -rf ${PGDATA}
rsync ALL=(root) NOPASSWD: /usr/bin/mkdir -p ${PGDATA}
rsync ALL=(root) NOPASSWD: /bin/mkdir -p ${PGDATA}
rsync ALL=(root) NOPASSWD: /usr/bin/chown postgres\:postgres ${PGDATA}
rsync ALL=(root) NOPASSWD: /bin/chown postgres\:postgres ${PGDATA}
rsync ALL=(root) NOPASSWD: /usr/bin/chmod 700 ${PGDATA}
rsync ALL=(root) NOPASSWD: /bin/chmod 700 ${PGDATA}

# ── find: verificar se PGDATA está vazio (sem permissão de leitura) ──────
rsync ALL=(root) NOPASSWD: /usr/bin/find ${PGDATA} *
rsync ALL=(root) NOPASSWD: /bin/find ${PGDATA} *

# ── check_system: subir servicos apos failback ──────────────────────────
# Argumento fixo -v com redirecionamento — nao permite outros comandos
rsync ALL=(root) NOPASSWD: /bin/bash -c /usr/local/rbenv/shims/ruby /home/totalip/ipserver/nagios/check_system.rb -v >> /var/log/failover.log 2>&1

# ── pkill: encerrar check_system anterior — argumento fixo ──────────────
rsync ALL=(root) NOPASSWD: /usr/bin/pkill -f check_system.rb
rsync ALL=(root) NOPASSWD: /bin/pkill -f check_system.rb
EOF

chmod 440 "$SUDOERS_FILE"

# ─────────────────────────────────────────────
# VALIDAR SINTAXE
# ─────────────────────────────────────────────
if visudo -c 2>/dev/null; then
    ok "Sudoers válido"
else
    rm -f "$SUDOERS_FILE"
    fail "Erro de sintaxe. Arquivo removido."
fi

# ─────────────────────────────────────────────
# LOG COM PERMISSAO PARA RSYNC ESCREVER
# ─────────────────────────────────────────────
touch /var/log/failover.log
chmod 666 /var/log/failover.log
ok "Log: /var/log/failover.log (666)"

# ─────────────────────────────────────────────
# RESUMO
# ─────────────────────────────────────────────
echo ""
echo -e "${GREEN}════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  NODE1 — Sudoers configurado com sucesso${NC}"
echo -e "${GREEN}════════════════════════════════════════════════${NC}"
echo ""
echo "  Arquivo  : $SUDOERS_FILE"
echo "  PG Ver   : ${PG_VER}"
echo "  PG Bin   : ${PG_BIN}"
echo "  PGDATA   : ${PGDATA}"
echo "  timeout  : ${TIMEOUT_BIN}"
echo "  runuser  : ${RUNUSER_BIN}"
echo ""
echo "  Comandos liberados para rsync (e somente esses):"
echo "    systemctl stop/start postgresql-${PG_VER}"
echo "    runuser postgres -- psql *"
echo "    runuser postgres -- pg_ctl *"
echo "    runuser postgres -- ${TIMEOUT_BIN} * pg_basebackup *"
echo "    rm/mkdir/chown/chmod ${PGDATA}"
echo "    find ${PGDATA} *"
echo "    bash -c check_system.rb -v >> /var/log/failover.log (argumento fixo)"
echo "    pkill check_system.rb"
echo ""
echo "  Validar  : visudo -c"
echo "  Listar   : sudo -l -U rsync"
echo ""