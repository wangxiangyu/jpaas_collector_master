class AddTableLogMonitorItem < ActiveRecord::Migration
    def change
        create_table :log_monitor_item do |t|
            t.string :raw_key
            t.string :item_name_prefix
            t.string :cycle
            t.string :match_str
            t.integer :threshold
            t.string :filter_str
            t.string :item_key 
            t.timestamps
        end
    end
end
