require 'net/ssh'
require 'net/sftp'
require 'active_support'
require 'action_view/helpers/number_helper'
require 'action_view/helpers/date_helper'

include ActionView::Helpers::NumberHelper
include ActionView::Helpers::DateHelper

def ssh_file_exists?(ssh, file_name)
  ssh.exec!('stat -c "%s" ~/' + file_name) do |ch, stream, data|
    if stream == :stderr || data.to_i == 0
      return false
    else
      return data.to_i
    end
  end
end

def do_dump(load = false, gzip = false)
  start_time = Time.now
  unless File.exists?('config.yml')
    puts 'Config file does not exist, exit'
    exit
  end

  auth = YAML.load(File.read('config.yml'))

  if auth['user'] && !auth['username']
    puts 'Old config format, please upgrade'
    exit
  end

  options = {:password => auth['password'], :auth_methods => %w(keyboard-interactive password)}
  options[:port] = auth['port'] if auth['port']
  dump_name = auth['database'] + '.' + Time.now.strftime('%d.%m.%Y.%H.%M') + '.sql'
  local_dump_name = File.join(File.dirname(File.expand_path(__FILE__)), '..', '..', 'tmp', dump_name)

  db_ignore_tables = nil
  if ignore_tables = auth['ignore_tables'] || auth['struct_only']
    db_ignore_tables = ' ' + (i = "--ignore-table=#{auth['database']}.") + ignore_tables.join(' ' + i)
  end

  actual_dump_size = 0

  Net::SSH.start(auth['host'], auth['username'], options) do |ssh|
    puts 'Dumping...'
    dump_size = 0
    ssh.exec!("#{auth['mysql_path'] || '/usr/bin/mysql'} -h #{auth['db_host']} -u #{auth['db_username']}#{' -p' + auth['db_password'] if auth['db_password']} " +
              "-e 'SELECT SUM(data_length) FROM information_schema.tables WHERE table_schema = \"#{auth['database']}\"'") do |ch, stream, data|
      dump_size = data.split("\n")[1].to_i
    end

    ssh.exec!((cmd = "#{auth['mysqldump_path'] || '/usr/bin/mysqldump'} --add-drop-table -h #{auth['db_host']} -u " +
                     "#{auth['db_username']}#{' -p' + auth['db_password'] if auth['db_password']} ") +
                     "#{db_ignore_tables} #{auth['database']}#{' ' + auth['only'].join(' ') if auth['only']} | " +
                     "#{auth['pv_path'] || '/usr/bin/pv'} -f -s #{dump_size} > #{dump_name}") do |ch, stream, data|
      print "\r" + data
    end

    if actual_dump_size = ssh_file_exists?(ssh, dump_name)
      puts 'Dump completed'
    else
      puts 'File not dumped'
      exit
    end

    auth['struct_only'].each do |struct_table|
      ssh.exec!("#{cmd} --no-data' #{auth['database']} #{struct_table} >> ~/#{dump_name}")
    end if auth['struct_only']

    if gzip
      puts 'Compressing...'
      ssh.exec!("#{auth['pv_path'] || '/usr/bin/pv'} -f ~/#{dump_name} | #{auth['gzip_path'] || '/usr/bin/gzip'} -9 > ~/#{dump_name}.gz") do |ch, stream, data|
        print "\r" + data
      end

      if ssh_file_exists?(ssh, dump_name + '.gz')
        puts 'Compression completed'
        ssh.exec!('rm ~/' + dump_name) if auth['drop_after']
      else
        puts 'File not compressed'
        gzip = false
      end
    end

    if auth['use_winscp']
      puts 'Downloading through WinSCP...'
      c = "#{auth['winscp_path']} /command \"open sftp://#{auth['username']}:#{auth['password']}@#{auth['host']}:#{auth['port']} -hostkey=\"\"#{auth['hostkey']}\"\"\"" +
                                         " \"get \"\"#{dump_name + (gzip ? '.gz' : '')}\"\" \"\"#{local_dump_name.gsub('/', '\\') + (gzip ? '.gz' : '')}\"\"\"" +
                                         '  "exit"'
      puts `#{c}`
      puts "Download completed in #{distance_of_time_in_words_to_now(start_time)} (#{Time.now.to_i - start_time.to_i} s)" if $?.exitstatus == 0
    else
      Net::SFTP.start(auth['host'], auth['username'], options) do |sftp|
        size = sftp.stat!(actual_dump_name = dump_name + (gzip ? '.gz' : '')).size
        progress_bar = nil
        sftp.download!(actual_dump_name, local_dump_name + (gzip ? '.gz' : '')) do |event, downloader, *args|
          case event
            when :open then
              progress_bar = ProgressBar.create(:title => "Downloading #{number_to_human_size(size, :precision => 2)}", :format => '%t %B %P% %e', :length => 100)
            when :get then
              progress_bar.progress = ((bytes = args[1] + args[2].length) / size.to_f * 100)
            when :finish then
              puts "Download completed in #{distance_of_time_in_words_to_now(start_time)} (#{Time.now.to_i - start_time.to_i} s)"
              sftp.remove!(actual_dump_name) if auth['drop_after']
          end
        end
      end
    end
  end

  if load
    start_time_load = Time.now
    #noinspection RubyNestedTernaryOperatorsInspection
    connection = auth['connection'] || (defined?(Rails) ? Rails.env : defined?(RAILS_ENV) ? RAILS_ENV : ENV['RAILS_ENV'] || 'development')
    #noinspection RubyResolve
    local_auth = YAML.load(File.read(File.join(File.dirname(File.expand_path(__FILE__)), '..', '..', 'config', 'database.yml')))[connection]
    if gzip
      puts 'Unpacking...'
      `gzip -d -c #{local_dump_name + '.gz'} | pv -f -s #{actual_dump_size} > #{local_dump_name}`
      puts 'File unpacked'
      File.delete(local_dump_name + '.gz') if auth['drop_after']
    end
    puts 'Loading...'
    cmd = "mysql -h #{local_auth['host']} -u #{local_auth['username']}#{' -p' + local_auth['password'] if local_auth['password']} " +
          "#{' -P' + local_auth['port'].to_s if local_auth['port']} #{local_auth['database']}"
    `pv -f #{File.expand_path(local_dump_name)} | #{cmd}`
    if $?.exitstatus == 0
      puts "Load completed in #{distance_of_time_in_words_to_now(start_time_load)} (#{Time.now.to_i - start_time_load.to_i} s)"
      File.delete(local_dump_name) if auth['drop_after']
    end
  end

  puts "Completed in #{distance_of_time_in_words_to_now(start_time)} (#{Time.now.to_i - start_time.to_i} s)"
end

desc 'Donwload DB dump'
task :dumper => :environment do
  do_dump
end

namespace :dumper do

  desc 'Donwload DB dump and load into local DB'
  task :load => :environment do
    do_dump(true)
  end

  desc 'Donwload gzipped DB dump'
  task :gzip => :environment do
    do_dump(false, true)
  end

  namespace :gzip do
    desc 'Donwload gzipped DB dump and load into local DB'
    task :load => :environment do
      do_dump(true, true)
    end
  end

end