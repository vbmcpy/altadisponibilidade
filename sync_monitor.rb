#!/usr/bin/env ruby
# sync_monitor.rb
# Propósito : Sync de baixa latência — só hoje e ontem
# Cron      : * * * * * flock -n /tmp/sync_monitor.lock bash -lc 'ruby /home/totalip/sync_monitor.rb' >> /var/log/sync_monitor.log 2>&1

require 'date'

LOG_FILE    = '/var/log/sync_monitor.log'
BASE_ORIGEM = '/var/spool/asterisk/monitor'
BASE_DESTINO= '/var/spool/asterisk/monitor'
REMOTE_HOST = '192.168.0.239'
REMOTE_USER = 'rsync'
SSH_KEY     = '/backup/id_rsync'
EXTENSOES   = %w[wav WAV gsm mp3]

# ControlMaster: reutiliza a mesma conexão SSH durante toda a execução
# evitando um handshake por diretório
SSH_CTL     = "/tmp/ssh_ctl_#{REMOTE_USER}@#{REMOTE_HOST}"
SSH_OPTS    = "-i #{SSH_KEY} -o StrictHostKeyChecking=no -o BatchMode=yes " \
              "-o ControlMaster=auto -o ControlPath=#{SSH_CTL} -o ControlPersist=120"

def log(msg)
  File.open(LOG_FILE, 'a') { |f| f.puts "[#{Time.now}] #{msg}" }
end

def ssh_exec(cmd)
  `ssh #{SSH_OPTS} #{REMOTE_USER}@#{REMOTE_HOST} #{Shellwords.escape(cmd)} 2>/dev/null`.chomp
end

def criar_dir_remoto(path)
  ssh_exec("mkdir -p '#{path}'")
end

# Apenas os dois dias relevantes para HA
def datas_alvo
  hoje = Date.today
  [hoje, hoje - 1]
end

def dirs_hora_do_dia(data)
  ano  = data.year.to_s
  mes  = data.month.to_s
  dia  = data.day.to_s
  base = "#{BASE_ORIGEM}/#{ano}/#{mes}/#{dia}"
  return [] unless Dir.exist?(base)

  Dir.entries(base)
     .reject { |e| e == '.' || e == '..' }
     .select { |e| File.directory?("#{base}/#{e}") }
     .map    { |hora| "#{base}/#{hora}" }
end

def sync_hora(hora_path)
  partes   = hora_path.sub("#{BASE_ORIGEM}/", '').split('/')
  destino  = "#{BASE_DESTINO}/#{partes.join('/')}"

  arquivos = Dir.glob("#{hora_path}/*.{#{EXTENSOES.join(',')}}")
  return if arquivos.empty?

  criar_dir_remoto(destino)

  # --ignore-existing: não retransferir o que já chegou (velocidade)
  # --size-only      : verificação rápida por tamanho (sem hash)
  # O verify_monitor.rb cuidará da integridade profunda à noite
  rsync_cmd = <<~CMD.chomp
    rsync -az \
    --ignore-existing \
    --size-only \
    --inplace \
    -e "ssh #{SSH_OPTS}" \
    "#{hora_path}/" \
    "#{REMOTE_USER}@#{REMOTE_HOST}:#{destino}/"
  CMD

  ok = system(rsync_cmd)
  log ok ? "OK: #{hora_path}" : "ERRO: #{hora_path}"
end

# ── Main ─────────────────────────────────────────────────────────────────────

datas_alvo.each do |data|
  dirs_hora_do_dia(data).each { |hora_path| sync_hora(hora_path) }
end

# Encerra o socket ControlMaster ao terminar
system("ssh -i #{SSH_KEY} -o ControlPath=#{SSH_CTL} -O exit #{REMOTE_USER}@#{REMOTE_HOST} 2>/dev/null")
