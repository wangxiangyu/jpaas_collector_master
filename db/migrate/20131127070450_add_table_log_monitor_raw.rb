class AddTableLogMonitorRaw < ActiveRecord::Migration
    def change
        create_table :log_monitor_raw do |t|
            t.string :app_key
            t.string :name
            t.string :cycle
            t.string :method
            t.string :target
            t.string :params
            t.string :log_filepath
            t.string :raw_key #app_key+log_filepath
            t.string :limit_rate
            t.timestamps
        end
    end
end
