#!/usr/bin/env ruby
# verify_monitor.rb
# Propósito : Verificação profunda de integridade + DB backup
# Cron      : 0 3 * * * flock -n /tmp/verify_monitor.lock bash -lc 'ruby /home/totalip/verify_monitor.rb' >> /var/log/verify_monitor.log 2>&1

require 'date'
require 'shellwords'

LOG_FILE     = '/var/log/verify_monitor.log'
BASE_ORIGEM  = '/var/spool/asterisk/monitor'
BASE_DESTINO = '/var/spool/asterisk/monitor'
REMOTE_HOST  = '192.168.0.239'
REMOTE_USER  = 'rsync'
SSH_KEY      = '/backup/id_rsync'
IGNORAR      = %w[tmp tmp_screen_records tmp_transcriptions]
DB_ORIGEM    = '/backup'
DB_DESTINO   = '/backup/backup_db'
DATA_HOJE    = Date.today
EXTENSOES    = %w[wav WAV gsm mp3]

SSH_CTL  = "/tmp/ssh_ctl_verify_#{REMOTE_USER}@#{REMOTE_HOST}"
SSH_OPTS = "-i #{SSH_KEY} -o StrictHostKeyChecking=no -o BatchMode=yes " \
           "-o ControlMaster=auto -o ControlPath=#{SSH_CTL} -o ControlPersist=300"

# ── Log ──────────────────────────────────────────────────────────────────────

def log(msg)
  File.open(LOG_FILE, 'a') { |f| f.puts "[#{Time.now}] #{msg}" }
end

# ── SSH / rsync helpers ───────────────────────────────────────────────────────

def ssh_exec(cmd)
  `ssh #{SSH_OPTS} #{REMOTE_USER}@#{REMOTE_HOST} #{Shellwords.escape(cmd)} 2>/dev/null`.chomp
end

def criar_dir_remoto(path)
  ssh_exec("mkdir -p '#{path}'")
end

def listar_dir_desc(path)
  Dir.entries(path)
     .reject { |e| e == '.' || e == '..' }
     .sort_by(&:to_i)
     .reverse
rescue
  []
end

# ── Verificação origem → destino ─────────────────────────────────────────────

def arquivos_locais(path)
  Dir.glob("#{path}/*.{#{EXTENSOES.join(',')}}").map { |f| File.basename(f) }
end

def arquivos_remotos(remote_path)
  raw = ssh_exec("ls '#{remote_path}/' 2>/dev/null")
  return [] if raw.empty?
  raw.split("\n").map(&:strip)
end

def rsync_faltando(local_path, remote_path, arquivos)
  return true if arquivos.empty?

  tmp = "/tmp/verify_missing_#{$$}.txt"
  File.write(tmp, arquivos.join("\n"))

  cmd = <<~CMD.chomp
    rsync -az \
    --files-from=#{tmp} \
    --checksum \
    --inplace \
    -e "ssh #{SSH_OPTS}" \
    "#{local_path}/" \
    "#{REMOTE_USER}@#{REMOTE_HOST}:#{remote_path}/"
  CMD

  ok = system(cmd)
  File.delete(tmp) rescue nil
  ok
end

# Verifica um diretório de hora, faz até `tentativas` rounds de correção.
# Retorna lista de arquivos que ficaram faltando após todas as tentativas.
def verificar_hora(local_path, remote_path, tentativas: 3)
  tentativas.times do |i|
    locais  = arquivos_locais(local_path)
    remotos = arquivos_remotos(remote_path)
    faltando = locais - remotos

    if faltando.empty?
      log "  OK (tentativa #{i + 1}): #{remote_path}" if i > 0
      return []
    end

    log "  #{faltando.size} faltando (tentativa #{i + 1}/#{tentativas}): #{remote_path}"
    faltando.each { |f| log "    FALTANDO: #{f}" }

    criar_dir_remoto(remote_path)
    ok = rsync_faltando(local_path, remote_path, faltando)
    log "  RETENTATIVA #{i + 1}: #{ok ? 'rsync OK' : 'rsync FALHOU'}"
  end

  # Checagem final
  locais   = arquivos_locais(local_path)
  remotos  = arquivos_remotos(remote_path)
  faltando = locais - remotos
  faltando.each { |f| log "  PENDENTE FINAL: #{f}" }
  faltando
end

# ── Backup DB ────────────────────────────────────────────────────────────────

def copiar_backups_db(origem, destino)
  log ">>> BACKUP DB"
  return log("ERRO: #{origem} inexistente") unless Dir.exist?(origem)

  criar_dir_remoto(destino)

  arquivos = Dir.glob("#{origem}/db*").select { |f| File.file?(f) }
  return log("ERRO: nenhum db* encontrado") if arquivos.empty?

  arquivos.each do |arquivo|
    cmd = <<~CMD.chomp
      rsync -az --checksum --no-o --no-g \
      -e "ssh #{SSH_OPTS}" \
      "#{arquivo}" \
      "#{REMOTE_USER}@#{REMOTE_HOST}:#{destino}/"
    CMD
    ok = system(cmd)
    log ok ? "DB OK: #{File.basename(arquivo)}" : "DB ERRO: #{File.basename(arquivo)}"
  end

  log "<<< BACKUP DB FINALIZADO"
end

# ── Verificação Asterisk ──────────────────────────────────────────────────────

def verificar_asterisk
  log ">>> VERIFICACAO ASTERISK (historico completo)"

  pendencias = {}   # { "local -> remoto" => [arquivos] }
  total_dirs = 0
  dirs_ok    = 0

  listar_dir_desc(BASE_ORIGEM).each do |ano|
    next if IGNORAR.include?(ano)
    ano_i    = ano.to_i
    ano_path = "#{BASE_ORIGEM}/#{ano}"
    next unless File.directory?(ano_path)
    next if ano_i > DATA_HOJE.year

    listar_dir_desc(ano_path).each do |mes|
      mes_i    = mes.to_i
      mes_path = "#{ano_path}/#{mes}"
      next unless File.directory?(mes_path)

      listar_dir_desc(mes_path).each do |dia|
        dia_i    = dia.to_i
        dia_path = "#{mes_path}/#{dia}"
        next unless File.directory?(dia_path)

        begin
          data_dir = Date.new(ano_i, mes_i, dia_i)
        rescue
          next
        end
        next if data_dir > DATA_HOJE

        listar_dir_desc(dia_path).each do |hora|
          hora_path = "#{dia_path}/#{hora}"
          next unless File.directory?(hora_path)

          locais = arquivos_locais(hora_path)
          next if locais.empty?

          total_dirs += 1
          destino = "#{BASE_DESTINO}/#{ano_i}/#{mes_i}/#{dia_i}/#{hora}"

          log "VERIFICANDO: #{hora_path}"

          # Rsync com --checksum para detectar arquivos corrompidos/incompletos
          rsync_cmd = <<~CMD.chomp
            rsync -az \
            --checksum \
            --ignore-existing \
            --inplace \
            --exclude=tmp \
            --exclude=tmp_screen_records \
            --exclude=tmp_transcriptions \
            -e "ssh #{SSH_OPTS}" \
            "#{hora_path}/" \
            "#{REMOTE_USER}@#{REMOTE_HOST}:#{destino}/"
          CMD

          criar_dir_remoto(destino)
          system(rsync_cmd)

          faltando = verificar_hora(hora_path, destino)

          if faltando.empty?
            dirs_ok += 1
          else
            pendencias["#{hora_path} -> #{destino}"] = faltando
          end
        end
      end
    end
  end

  log ""
  log "─── RESUMO VERIFICACAO ───────────────────────────────"
  log "Diretorios verificados : #{total_dirs}"
  log "OK                     : #{dirs_ok}"
  log "Com pendencias         : #{pendencias.size}"

  if pendencias.any?
    pendencias.each do |par, arquivos|
      log "  PENDENTE: #{par} (#{arquivos.size} arquivo(s))"
    end
  end

  log "──────────────────────────────────────────────────────"
  log "<<< VERIFICACAO ASTERISK FINALIZADA"
end

# ── Main ─────────────────────────────────────────────────────────────────────

log "=" * 60
log "INICIANDO VERIFY_MONITOR"

copiar_backups_db(DB_ORIGEM, DB_DESTINO)
verificar_asterisk

log "VERIFY_MONITOR CONCLUIDO"
log "=" * 60

# Encerra socket ControlMaster
system("ssh -i #{SSH_KEY} -o ControlPath=#{SSH_CTL} -O exit #{REMOTE_USER}@#{REMOTE_HOST} 2>/dev/null")
