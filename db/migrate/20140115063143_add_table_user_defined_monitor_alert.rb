class AddTableUserDefinedMonitorAlert < ActiveRecord::Migration
    def change
        create_table :user_defined_monitor_alarm do |t|
            t.string :raw_key
            t.string :name
            t.string :max_alert_times
            t.string :remind_interval_second
            t.string :mail
            t.string :sms
            t.timestamps
        end
    end
end
