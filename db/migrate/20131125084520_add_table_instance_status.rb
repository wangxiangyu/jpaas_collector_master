class AddTableInstanceStatus < ActiveRecord::Migration
	def change
    		create_table :instance_status do |t|
		t.integer :time
		t.string :host 
		t.string :app_name
		t.integer :instance_index
		t.string :cluster_num
		t.string :organization
		t.string :space
		t.string :bns_node
		t.string :uris
		t.string :state
		t.string :warden_handle
		t.string :warden_container_path
		t.string :state_starting_timestamp
		t.text :port_info
      		t.timestamps
    end
  end
end
