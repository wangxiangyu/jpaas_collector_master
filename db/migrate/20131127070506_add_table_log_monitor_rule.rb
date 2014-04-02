class AddTableLogMonitorRule < ActiveRecord::Migration
    def change
        create_table :log_monitor_rule do |t|
            t.string :item_key
            t.string :name
            t.string :compare
            t.string :threshold
            t.string :filter
            t.string :alert
            t.string :disable_time
            t.string :rule_key
            t.timestamps
        end
    end
end
