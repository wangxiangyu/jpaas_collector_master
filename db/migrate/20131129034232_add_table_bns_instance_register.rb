class AddTableBnsInstanceRegister < ActiveRecord::Migration
    def change
        create_table :bns_instance_register do |t|
            t.string :app_key
            t.string :cluster_num
            t.string :instance_index
            t.string :bns_instance_id
            t.string :host
            t.string :instance_key
            t.timestamps
        end
    end
end
