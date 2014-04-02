class AddNoahmonitorportToInstanceStatus < ActiveRecord::Migration
    def change
        change_table :instance_status do |t|
            t.string :noah_monitor_port
        end
    end
end
