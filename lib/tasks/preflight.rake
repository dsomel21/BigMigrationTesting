namespace :db do
  desc "Simple DB preflight diagnostics: db:preflight[table_name]"
  task :preflight, [:table] => :environment do |t, args|
    table = (args[:table] || '').to_s.strip

    conn = ActiveRecord::Base.connection

    puts "=" * 80
    puts "DB Preflight"
    puts "Started: #{Time.now.strftime('%Y-%m-%d %H:%M:%S')}"
    puts "-" * 80

    # Server diagnostics
    begin
      version = conn.select_value('SELECT VERSION()')
      db_name = conn.current_database
      binlog_format = conn.select_value("SELECT @@GLOBAL.binlog_format") rescue 'N/A'
      log_bin_trust = conn.select_value("SELECT @@GLOBAL.log_bin_trust_function_creators") rescue 'N/A'
      read_only = conn.select_value("SELECT @@GLOBAL.read_only") rescue 'N/A'
      puts "MySQL version: #{version}"
      puts "Database: #{db_name}"
      puts "binlog_format: #{binlog_format} | log_bin_trust_function_creators: #{log_bin_trust} | read_only: #{read_only}"
    rescue => e
      puts "Server diagnostics error: #{e.class}: #{e.message}"
    end

    if table.empty?
      puts "-" * 80
      puts "Tip: Provide a table to inspect indexes/engine. Example:"
      puts "  bundle exec rake 'db:preflight[transactions]'"
      puts "=" * 80
      next
    end

    # Table diagnostics
    begin
      quoted_table = conn.quote(table)
      tbl = conn.select_all(<<~SQL).to_a.first
        SELECT TABLE_NAME, ENGINE
        FROM information_schema.TABLES
        WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = #{quoted_table}
      SQL

      if tbl.nil?
        puts "-" * 80
        puts "Table '#{table}' not found in '#{conn.current_database}'."
        puts "=" * 80
        next
      end

      engine = tbl['ENGINE']
      puts "-" * 80
      puts "Table: #{table} | Engine: #{engine}"

      # Primary key
      pk_cols = conn.select_all(<<~SQL).to_a
        SELECT k.COLUMN_NAME
        FROM information_schema.TABLE_CONSTRAINTS tc
        JOIN information_schema.KEY_COLUMN_USAGE k
          ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
         AND k.TABLE_SCHEMA = tc.TABLE_SCHEMA
         AND k.TABLE_NAME = tc.TABLE_NAME
        WHERE tc.TABLE_SCHEMA = DATABASE()
          AND tc.TABLE_NAME   = #{quoted_table}
          AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
        ORDER BY k.ORDINAL_POSITION
      SQL

      if pk_cols.any?
        puts "Primary key: (#{pk_cols.map { |r| r['COLUMN_NAME'] }.join(', ')})"
      else
        puts "Primary key: NONE"
      end

      # Unique indexes
      uniques = conn.select_all(<<~SQL).to_a
        SELECT s.INDEX_NAME, GROUP_CONCAT(s.COLUMN_NAME ORDER BY s.SEQ_IN_INDEX) AS cols
        FROM information_schema.STATISTICS s
        WHERE s.TABLE_SCHEMA = DATABASE()
          AND s.TABLE_NAME   = #{quoted_table}
          AND s.NON_UNIQUE = 0
        GROUP BY s.INDEX_NAME
        ORDER BY s.INDEX_NAME
      SQL

      if uniques.any?
        puts "Unique indexes:"
        uniques.each { |r| puts "  - #{r['INDEX_NAME']}: (#{r['cols']})" }
      else
        puts "Unique indexes: NONE"
      end
    rescue => e
      puts "Table diagnostics error: #{e.class}: #{e.message}"
    end

    puts "=" * 80
  end
end


