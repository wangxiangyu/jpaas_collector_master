class AddContainerIpToInstanceStatus < ActiveRecord::Migration
    def change
        change_table :instance_status do |t|
            t.string :warden_host_ip
        end
    end
end
