class AddVersionToBnsInstanceRegister < ActiveRecord::Migration
    def change
        change_table :bns_instance_register do |t|
            t.string :version
        end
    end
end
