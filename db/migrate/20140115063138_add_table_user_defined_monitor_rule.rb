class AddTableUserDefinedMonitorRule < ActiveRecord::Migration
    def change
        create_table :user_defined_monitor_rule do |t|
            t.string :raw_key
            t.string :name
            t.string :monitor_item
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
