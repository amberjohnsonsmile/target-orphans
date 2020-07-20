require 'aws-sdk'
require 'logger'
require 'pry'
require 'ruby-progressbar'

class CountOrphans

  def self.dynamodb
    @dynamodb ||= Aws::DynamoDB::Client.new(region: 'us-east-1')
  end

  # Pull a set of items
  def self.get_items(exclusive_start_key)
    request_params = {
      table_name: 'CustomerLoyaltyAccount',
    }

    request_params.merge!(exclusive_start_key)
    dynamodb.scan(request_params)
  end

  # Filter for only rid 3 (Target instore)
  def self.get_target_items(items)
    items.select { |i| i['rid'] == 3 }
  end

  def self.count_items(items, orphans, log)
    items.each do |item|
      orphans = count_item(item, orphans, log)
    end
    return orphans
  end

  def self.count_item(item, orphans, log)
    existing_params = {
      table_name: 'CustomerLoyaltyAccount',
      key: {
          'cid': item['cid'],
          'rid': 1433
      }
    }

    existing_item = dynamodb.get_item(existing_params)
    if existing_item['item'].nil?
      orphans += 1
      log.warn(item)
    end
    return orphans
  end

  def self.count_target_items
    log = Logger.new('log.txt')

    # This value is hard-coded as of 7/1/20
    progressbar = ProgressBar.create(:total => 3737727, :format => "%a %B %P%% %t")

    starting_point = {:exclusive_start_key=>{"cid"=>0.10615705e8, "rid"=>0.1433e4}}
    # starting_point = {}
    response = get_items(starting_point)

    total_items = 0
    orphans = 0

    while response.last_evaluated_key
      puts "Items in this batch: #{response.items.size}\n"
      items_to_count = get_target_items(response.items)

      if !items_to_count.empty?
        orphans = count_items(items_to_count, orphans, log)
      else
        puts "No Target links to count in this batch\n"
      end

      response.items.size.times { progressbar.increment }
      total_items += response.items.size
      puts "Total items scanned: #{total_items}"

      exclusive_start_key = { exclusive_start_key: response.last_evaluated_key }
      puts "\nNew starting point: #{exclusive_start_key}\n"
      puts "\nOrphans found: #{orphans}\n"

      response = get_items(exclusive_start_key)
    end

    puts "Finished."
  end

end

CountOrphans.count_target_items
