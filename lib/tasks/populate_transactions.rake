namespace :db do
  desc "Populate transactions table with specified number of records (batch) or stream 1/sec"
  task :populate_transactions, [:count, :mode, :start_from, :interval_seconds] => :environment do |t, args|
    # Helper method to format duration
    def format_duration(seconds)
      hours = (seconds / 3600).to_i
      minutes = ((seconds % 3600) / 60).to_i
      secs = (seconds % 60).to_i
      
      parts = []
      parts << "#{hours}h" if hours > 0
      parts << "#{minutes}m" if minutes > 0 || hours > 0
      parts << "#{secs}s"
      
      parts.join(' ')
    end
    
    count = args[:count]&.to_s&.strip
    count = (count.nil? || count.empty?) ? nil : count.to_i
    mode = (args[:mode] || 'batch').to_s
    start_from = args[:start_from]&.to_s&.strip
    start_from = (start_from.nil? || start_from.empty?) ? 1 : start_from.to_i
    interval_seconds = args[:interval_seconds]&.to_s&.strip
    interval_seconds = (interval_seconds.nil? || interval_seconds.empty?) ? 1.0 : interval_seconds.to_f
    
    if mode == 'batch'
      if count.nil? || count <= 0
        puts "ERROR: In batch mode, count must be a positive number"
        exit 1
      end
    end
    
    puts "=" * 80
    puts "Starting Transaction Population Task"
    puts "=" * 80
    if mode == 'batch'
      puts "Mode: batch"
      puts "Target: #{count.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} transactions"
    else
      puts "Mode: stream (1 record every #{interval_seconds}s)"
      puts "Starting from counter: #{start_from.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse}"
      puts "Limit: #{count ? count.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse : 'none (run until stopped)'}"
    end
    puts "Started at: #{Time.now.strftime('%Y-%m-%d %H:%M:%S')}"
    puts "-" * 80
    
    # Configuration - adjust batch size based on total count (batch mode only)
    BATCH_SIZE = (mode == 'batch' && count && count < 1_000) ? 100 : 10_000
    DELAY_BETWEEN_BATCHES = 0.1  # 100ms delay between batches (adjust as needed)
    PROGRESS_UPDATE_INTERVAL = (mode == 'batch' && count && count < 1_000) ? 50 : 10_000
    
    start_time = Time.now
    if mode == 'stream'
      begin
        created_count = 0
        current_value = start_from
        last_log = Time.now
        log_every = 10  # seconds
        start_time = Time.now

        puts "Streaming inserts... Press Ctrl-C to stop."
        puts "-" * 80

        loop do
          now = Time.now
          Transaction.create!(name: "Transiaction #{current_value}")
          created_count += 1
          current_value += 1

          # Optional limit if count provided
          break if count && created_count >= count

          # Periodic log
          if (now - last_log) >= log_every
            elapsed = now - start_time
            rate = elapsed > 0 ? (created_count / elapsed) : 0
            puts "[#{Time.now.strftime('%H:%M:%S')}] Streamed so far: #{created_count.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} | Current name: Transiaction #{current_value - 1} | Rate: #{rate.round(2)} rec/sec"
            last_log = now
          end

          sleep(interval_seconds)
        end

        end_time = Time.now
        total_elapsed = end_time - start_time
        final_rate = total_elapsed > 0 ? (created_count / total_elapsed) : 0

        puts "-" * 80
        puts "Stream finished"
        puts "Created: #{created_count.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} rows"
        puts "Duration: #{format_duration(total_elapsed)}"
        puts "Average Rate: #{final_rate.round(2)} rec/sec"
        puts "=" * 80
      rescue => e
        puts "=" * 80
        puts "ERROR: Stream failed!"
        puts "=" * 80
        puts "Error: #{e.class}: #{e.message}"
        puts e.backtrace.first(10).join("\n")
        puts "-" * 80
        raise
      end
    else
      created_count = 0
      total_batches = (count / BATCH_SIZE.to_f).ceil

      # Calculate how many batches before a progress update
      batches_per_update = [1, (PROGRESS_UPDATE_INTERVAL / BATCH_SIZE.to_f).ceil].max

      begin
        (1..total_batches).each do |batch_num|
          # Calculate how many records to create in this batch
          remaining = count - created_count
          batch_count = [BATCH_SIZE, remaining].min

          # Generate batch data
          batch_data = []
          (1..batch_count).each do |i|
            # Keep previous batch naming for batch mode
            batch_data << {
              name: "Transaction #{created_count + i}",
              created_at: Time.now,
              updated_at: Time.now
            }
          end

          # Bulk insert using insert_all for better performance
          Transaction.insert_all(batch_data)
          created_count += batch_count

          # Show progress at regular intervals
          if batch_num % batches_per_update == 0 || batch_num == total_batches || created_count >= count
            elapsed = Time.now - start_time
            remaining_count = count - created_count
            percentage = (created_count.to_f / count * 100).round(2)
            rate = elapsed > 0 ? (created_count / elapsed) : 0  # records per second

            # Calculate ETA
            eta_seconds = (remaining_count > 0 && rate > 0) ? (remaining_count / rate).round : 0
            if eta_seconds > 0
              hours = (eta_seconds / 3600).to_i
              minutes = ((eta_seconds % 3600) / 60).to_i
              secs = (eta_seconds % 60).to_i
              eta_parts = []
              eta_parts << "#{hours}h" if hours > 0
              eta_parts << "#{minutes}m" if minutes > 0 || hours > 0
              eta_parts << "#{secs}s"
              eta_str = eta_parts.join(' ')
            else
              eta_str = "Complete"
            end

            puts "[#{Time.now.strftime('%H:%M:%S')}] Progress: #{created_count.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} / #{count.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} (#{percentage}%)"
            puts "         Remaining: #{remaining_count.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} | Rate: #{rate.round(0).to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} rec/sec | ETA: #{eta_str}"
            puts "-" * 80
          end

          # Delay between batches (unless we're done)
          if created_count < count && DELAY_BETWEEN_BATCHES > 0
            sleep(DELAY_BETWEEN_BATCHES)
          end

          # Safety check to prevent exceeding count
          break if created_count >= count
        end

        # Final summary
        end_time = Time.now
        total_elapsed = end_time - start_time
        final_rate = total_elapsed > 0 ? (created_count / total_elapsed) : 0

        puts "=" * 80
        puts "Task Completed Successfully!"
        puts "=" * 80
        puts "Created: #{created_count.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} transactions"
        puts "Started:  #{start_time.strftime('%Y-%m-%d %H:%M:%S')}"
        puts "Finished: #{end_time.strftime('%Y-%m-%d %H:%M:%S')}"
        puts "Duration: #{format_duration(total_elapsed)}"
        puts "Average Rate: #{final_rate.round(0).to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} records/second"
        puts "=" * 80

      rescue => e
        puts "=" * 80
        puts "ERROR: Task failed!"
        puts "=" * 80
        puts "Error: #{e.class}: #{e.message}"
        puts "Backtrace:"
        puts e.backtrace.first(10).join("\n")
        puts "-" * 80
        puts "Successfully created #{created_count.to_s.reverse.gsub(/(\d{3})(?=\d)/, '\\1,').reverse} transactions before error"
        puts "=" * 80
        raise
      end
    end
  end
end

