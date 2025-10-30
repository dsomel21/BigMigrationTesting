namespace :db do
  desc "Preflight checks and prepare online copy with triggers: db:online_copy_prepare[table_name,new_suffix]"
  task :online_copy_prepare, [:table, :new_suffix] => :environment do |t, args|
    # Inputs
    table = args[:table].to_s.strip
    if table.empty?
      puts "ERROR: Please provide table name. Usage: rake 'db:online_copy_prepare[transactions]'"
      exit 1
    end
    new_suffix = (args[:new_suffix] || 'new').to_s.strip

    conn = ActiveRecord::Base.connection
    db_name = conn.current_database

    puts "=" * 80
    puts "Online Copy Preflight"
    puts "DB: #{db_name} | Table: #{table} | New suffix: #{new_suffix}"
    puts "Started at: #{Time.now.strftime('%Y-%m-%d %H:%M:%S')}"
    puts "-" * 80

    # Helper to run a select and return rows as hashes
    def select_all_hashes(sql)
      ActiveRecord::Base.connection.select_all(sql).to_a
    end

    # 1) Confirm table exists and engine is InnoDB
    tbl = select_all_hashes(<<~SQL).first
      SELECT TABLE_NAME, ENGINE
      FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = #{ActiveRecord::Base.connection.quote(table)}
    SQL

    unless tbl
      puts "ERROR: Table '#{table}' does not exist in schema '#{db_name}'."
      puts diagnostics_sql(db_name)
      exit 1
    end
    engine = tbl["ENGINE"]
    puts "Engine: #{engine}"

    # 2) Detect Primary Key or Unique index
    pk = select_all_hashes(<<~SQL)
      SELECT k.CONSTRAINT_NAME, k.COLUMN_NAME
      FROM information_schema.TABLE_CONSTRAINTS tc
      JOIN information_schema.KEY_COLUMN_USAGE k
        ON k.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
       AND k.TABLE_SCHEMA = tc.TABLE_SCHEMA
       AND k.TABLE_NAME = tc.TABLE_NAME
      WHERE tc.TABLE_SCHEMA = DATABASE()
        AND tc.TABLE_NAME   = #{conn.quote(table)}
        AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
      ORDER BY k.ORDINAL_POSITION
    SQL

    unique_keys = select_all_hashes(<<~SQL)
      SELECT s.INDEX_NAME, s.COLUMN_NAME, s.NON_UNIQUE
      FROM information_schema.STATISTICS s
      WHERE s.TABLE_SCHEMA = DATABASE()
        AND s.TABLE_NAME   = #{conn.quote(table)}
        AND s.NON_UNIQUE = 0
      ORDER BY s.INDEX_NAME, s.SEQ_IN_INDEX
    SQL

    candidate_key = nil
    if pk && !pk.empty?
      candidate_key = { name: 'PRIMARY', columns: pk.map { |r| r["COLUMN_NAME"] } }
      puts "Primary key found: (#{candidate_key[:columns].join(', ')})"
    elsif unique_keys && !unique_keys.empty?
      # Take the first unique index as candidate
      grouped = unique_keys.group_by { |r| r["INDEX_NAME"] }
      name, rows = grouped.first
      candidate_key = { name: name, columns: rows.map { |r| r["COLUMN_NAME"] } }
      puts "Unique index found: #{name} (#{candidate_key[:columns].join(', ')})"
    else
      puts "ERROR: No PRIMARY KEY or UNIQUE index found. Cannot safely mirror UPDATE/DELETE."
      puts fallback_diagnostics(db_name, table)
      exit 1
    end

    # 3) Create copy table with brief metadata lock
    new_table = "#{table}_#{new_suffix}"
    exists_new = select_all_hashes(<<~SQL).first
      SELECT 1 FROM information_schema.TABLES
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = #{conn.quote(new_table)}
    SQL

    if exists_new
      puts "Notice: Target table '#{new_table}' already exists. Skipping CREATE TABLE LIKE."
    else
      puts "Creating copy table '#{new_table}' via CREATE TABLE ... LIKE (brief MDL)."
      conn.execute("CREATE TABLE #{conn.quote_table_name(new_table)} LIKE #{conn.quote_table_name(table)}")
      puts "Created table '#{new_table}'."
    end

    # 4) Add triggers to mirror changes
    # Build WHERE and column lists for triggers
    key_columns = candidate_key[:columns]
    where_old = key_columns.map { |c| "`#{c}` = OLD.`#{c}`" }.join(' AND ')
    where_new = key_columns.map { |c| "`#{c}` = NEW.`#{c}`" }.join(' AND ')

    # Fetch column list for full-row operations
    cols = select_all_hashes(<<~SQL)
      SELECT COLUMN_NAME
      FROM information_schema.COLUMNS
      WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = #{conn.quote(table)}
      ORDER BY ORDINAL_POSITION
    SQL
    column_names = cols.map { |r| "`#{r["COLUMN_NAME"]}`" }

    insert_columns = column_names.join(', ')
    insert_values  = cols.map { |r| "NEW.`#{r["COLUMN_NAME"]}`" }.join(', ')

    # Trigger names
    trig_ins = "#{table}_mirror_ins"
    trig_upd = "#{table}_mirror_upd"
    trig_del = "#{table}_mirror_del"

    # Drop existing triggers if present (idempotent setup)
    [trig_ins, trig_upd, trig_del].each do |tr|
      conn.execute("DROP TRIGGER IF EXISTS #{conn.quote_table_name(tr)}")
    end

    puts "Creating triggers to mirror INSERT/UPDATE/DELETE to '#{new_table}'."

    conn.execute(<<~SQL)
      CREATE TRIGGER #{conn.quote_table_name(trig_ins)}
      AFTER INSERT ON #{conn.quote_table_name(table)}
      FOR EACH ROW
      INSERT INTO #{conn.quote_table_name(new_table)} (#{insert_columns})
      VALUES (#{insert_values});
    SQL

    # For UPDATE: upsert into new table to keep row current
    # Use single-statement upsert to avoid compound triggers (helps in restricted environments)
    upsert_update_assignments = column_names.map { |c| "#{c} = VALUES(#{c})" }.join(', ')
    conn.execute(<<~SQL)
      CREATE TRIGGER #{conn.quote_table_name(trig_upd)}
      AFTER UPDATE ON #{conn.quote_table_name(table)}
      FOR EACH ROW
      INSERT INTO #{conn.quote_table_name(new_table)} (#{insert_columns})
      VALUES (#{insert_values})
      ON DUPLICATE KEY UPDATE #{upsert_update_assignments};
    SQL

    conn.execute(<<~SQL)
      CREATE TRIGGER #{conn.quote_table_name(trig_del)}
      AFTER DELETE ON #{conn.quote_table_name(table)}
      FOR EACH ROW
      DELETE FROM #{conn.quote_table_name(new_table)}
      WHERE #{where_old};
    SQL

    puts "-" * 80
    puts "Preflight OK. Copy table and triggers ready."
    puts "New table: #{new_table}"
    puts "Mirroring key: (#{key_columns.join(', ')})"
    puts "=" * 80
  rescue => e
    puts "=" * 80
    puts "ERROR during online copy prepare"
    puts "Error: #{e.class}: #{e.message}"
    puts e.backtrace.first(10).join("\n")
    puts "-" * 80
    puts fallback_diagnostics(db_name, table)
    raise
  end
end

# Fallback diagnostics helper
def fallback_diagnostics(db_name, table)
  conn = ActiveRecord::Base.connection
  info = []
  info << "Diagnostics:"
  info << "MySQL version: #{conn.select_value('SELECT VERSION()')}"
  info << "Current database: #{db_name}"
  info << "Read only: #{conn.select_value('SELECT @@global.read_only')} | Binlog format: #{conn.select_value("SELECT @@global.binlog_format") rescue 'N/A'}"
  info << "innodb_flush_log_at_trx_commit: #{conn.select_value('SELECT @@innodb_flush_log_at_trx_commit')} | sync_binlog: #{conn.select_value('SELECT @@sync_binlog')}"
  tbl_status = conn.select_all("SHOW TABLE STATUS LIKE #{conn.quote(table)}").to_a.first
  if tbl_status
    info << "Table rows (estimate): #{tbl_status['Rows']} | Engine: #{tbl_status['Engine']} | Row_format: #{tbl_status['Row_format']}"
  end
  info.join("\n")
end

def diagnostics_sql(db_name)
  <<~TXT
  Diagnostics:
  - Current database: #{db_name}
  - Tip: Verify table name and permissions
  - SHOW TABLES; (to list tables)
  - SELECT VERSION(); (to display server version)
  TXT
end


