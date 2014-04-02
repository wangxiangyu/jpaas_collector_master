class AddInstanceIdToInstanceStatus < ActiveRecord::Migration
  def change
      change_table :instance_status do |t|
          t.string :instance_id
      end
  end
end
