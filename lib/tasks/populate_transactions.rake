namespace :transactions do
  desc "Populate transactions table with 100 million rows"
  task populate: :environment do
    total_rows = 100_000_000
    batch_size = 10_000
    total_batches = total_rows / batch_size
    
    puts "Starting to populate #{total_rows} transactions..."
    puts "Batch size: #{batch_size}"
    puts "Total batches: #{total_batches}"
    
    start_time = Time.now
    
    total_batches.times do |batch_num|
      records = []
      batch_size.times do |i|
        record_num = (batch_num * batch_size) + i + 1
        records << { name: "Transaction #{record_num}", created_at: Time.current, updated_at: Time.current }
      end
      
      # Use insert_all for bulk insert (much faster than individual creates)
      Transaction.insert_all(records)
      
      if (batch_num + 1) % 100 == 0
        elapsed = Time.now - start_time
        completed = (batch_num + 1) * batch_size
        rate = completed / elapsed
        remaining = total_rows - completed
        eta_seconds = remaining / rate
        eta_minutes = eta_seconds / 60
        
        puts "Progress: #{completed}/#{total_rows} (#{((completed.to_f / total_rows) * 100).round(2)}%)"
        puts "  Rate: #{rate.to_i} rows/sec, ETA: #{eta_minutes.round} minutes"
        puts "  Elapsed: #{(elapsed / 60).round} minutes"
        puts "---"
      end
    end
    
    elapsed = Time.now - start_time
    puts "\nCompleted! Inserted #{total_rows} transactions in #{elapsed.round} seconds (#{(elapsed / 60).round(2)} minutes)"
    puts "Average rate: #{(total_rows / elapsed).to_i} rows/sec"
  end
end
