class AddTableProcMonitorRaw < ActiveRecord::Migration
    def change
        create_table :proc_monitor_raw do |t|
            t.string :app_key
            t.string :name
            t.string :cycle
            t.string :method
            t.string :target
            t.string :params
            t.string :raw_key
            t.timestamps
        end
    end
end
