# GMUD — Gerenciamento de Mudanças
## Implantação de Script de Alta Disponibilidade PostgreSQL

**Sistema:** TotalIP  
**Componente:** Banco de dados PostgreSQL 16  
**Tipo de mudança:** Implementação de automação de failover/failback  

---

## Descrição da Mudança

Implantação de script automatizado (`pg-failover.sh`) para gerenciamento de alta disponibilidade do banco de dados PostgreSQL, com capacidade de failover automático em caso de falha do servidor primário e failback controlado para restauração do ambiente original.

---

## Motivação

O ambiente atual de dois servidores PostgreSQL com replicação streaming não possui automação de failover. Em caso de falha do servidor primário, a intervenção manual necessária pode levar de 15 a 60 minutos, impactando diretamente a disponibilidade dos serviços. A solução proposta reduz esse tempo para menos de 30 segundos.

---

## Escopo da Mudança

**Servidor impactado:** Node B (10.11.12.239) — instalação e execução do script  
**Servidor monitorado:** Node A (10.11.12.81) — monitorado remotamente via SSH  
**Banco de dados:** PostgreSQL 16  
**Banco afetado:** totalipdb  

**O que é instalado:**
- Script `/usr/local/bin/pg-failover.sh` no Node B
- Serviço systemd `pg-failover.service` no Node B
- Arquivo de credenciais `/etc/pg-failover.env` no Node B (permissão 600)
- Regras sudoers para usuário `rsync` no Node A

**O que não é alterado:**
- Configuração do PostgreSQL
- Dados do banco
- Configuração de replicação existente
- Outros serviços em execução

---

## Análise de Riscos

### Risco 1 — Failover indevido (falso positivo)
**Probabilidade:** Baixa  
**Impacto:** Alto  
**Descrição:** O script pode interpretar uma instabilidade temporária de rede como falha do Node A e promover o Node B desnecessariamente, causando uma troca de master não planejada.  
**Mitigação implementada:** O failover só é disparado após 3 checks consecutivos falhos em 3 camadas independentes (ping + SSH + PostgreSQL), totalizando aproximadamente 15 segundos de confirmação. Uma instabilidade momentânea de rede não aciona o failover.

### Risco 2 — Split-brain (dois masters simultâneos)
**Probabilidade:** Baixa  
**Impacto:** Muito Alto  
**Descrição:** Se o Node A retornar ao ar com o PostgreSQL rodando como master enquanto o Node B ainda é master, ambos podem aceitar escritas simultaneamente, causando divergência de dados.  
**Mitigação implementada:** O script detecta automaticamente o split-brain quando A retorna. Imediatamente força o PostgreSQL de A em modo read-only e em seguida o para completamente antes de iniciar o processo de failback. O Node B continua sendo o único master aceitando escritas durante todo o processo.

### Risco 3 — Perda de dados durante o failover
**Probabilidade:** Muito Baixa  
**Impacto:** Alto  
**Descrição:** Se o Node B estiver com lag de replicação alto no momento do failover, dados recentes gravados no Node A e ainda não replicados serão perdidos.  
**Mitigação implementada:** O script verifica o lag de replicação antes de executar o failback. Para o failover, como o Node A está offline, o script alerta via Telegram caso o lag seja alto, mas prossegue pois não há outra opção. A mitigação definitiva é manter a replicação síncrona ativa (synchronous_commit), garantindo que toda escrita só é confirmada após ser replicada.

### Risco 4 — Falha no processo de failback
**Probabilidade:** Baixa  
**Impacto:** Médio  
**Descrição:** O processo de failback envolve parar o Node A, apagar seus dados, recriar via pg_basebackup e promover. Se qualquer etapa falhar no meio, o ambiente pode ficar em estado inconsistente.  
**Mitigação implementada:** O script possui mecanismo de rollback automático em todas as etapas críticas. Se qualquer passo falhar, o Node B é restaurado como master e o script grava um flag de erro que bloqueia novas tentativas até intervenção manual. Notificação Telegram é enviada em todos os cenários de falha.

### Risco 5 — Indisponibilidade durante o failback
**Probabilidade:** Alta (evento esperado)  
**Impacto:** Médio  
**Descrição:** O processo de failback causa uma breve indisponibilidade do banco quando o Node B é parado para receber o pg_basebackup do Node A restaurado.  
**Mitigação implementada:** O failback é executado em horário agendado (padrão: 02:00), em janela de baixo uso. A indisponibilidade é de segundos (apenas o tempo de parar o PostgreSQL do Node B e subir como replica). O pg_basebackup de B a partir de A ocorre com o Node A já como master e aceitando conexões.

### Risco 6 — Disco cheio durante pg_basebackup
**Probabilidade:** Baixa  
**Impacto:** Alto  
**Descrição:** O pg_basebackup copia todos os dados do banco. Se o disco do destino não tiver espaço suficiente, o processo falha no meio, deixando o PGDATA de destino incompleto.  
**Mitigação implementada:** O pre-flight check verifica o espaço livre em ambos os servidores antes de iniciar qualquer operação. O mínimo configurado é 5GB livres. Se insuficiente, o failback é abortado antes de qualquer modificação.

### Risco 7 — Dependência de conectividade SSH
**Probabilidade:** Baixa  
**Impacto:** Alto  
**Descrição:** O script depende de SSH entre os servidores para executar comandos remotos. Falha de SSH impede o failback automatizado.  
**Mitigação implementada:** O SSH usa chave privada dedicada do usuário `rsync`, sem senha, com timeout configurado. O script possui retry com backoff exponencial (3 tentativas). Em caso de falha persistente de SSH, o failback não é executado e alerta é enviado via Telegram para intervenção manual.

---

## Plano de Rollback da GMUD

Caso a implantação precise ser revertida:

```bash
# No NODE B — parar e desabilitar o serviço
systemctl stop pg-failover
systemctl disable pg-failover

# Remover arquivos instalados
rm -f /usr/local/bin/pg-failover.sh
rm -f /etc/systemd/system/pg-failover.service
rm -f /etc/pg-failover.env
systemctl daemon-reload

# No NODE A — remover sudoers
rm -f /etc/sudoers.d/pg-failover-remote
visudo -c
```

O rollback não afeta o PostgreSQL, os dados ou a replicação em curso.

---

## Critérios de Sucesso

- Serviço `pg-failover` ativo e sem erros no log
- Em simulação de falha: Node B assume master em até 30 segundos
- Em simulação de retorno: Node A assume master no horário agendado
- Dados consistentes após o ciclo completo de failover e failback
- Notificações Telegram recebidas em todos os eventos

---

## Responsável Técnico

**Implementação:** Victor Coelho  
**Ambiente:** TotalIP  
**Data:** Abril 2026  
