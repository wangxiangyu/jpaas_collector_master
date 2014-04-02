class AddInstanceResourceQuotaToInstanceStatus < ActiveRecord::Migration
  def change
      change_table :instance_status do |t|
          t.string :disk_quota
          t.string :mem_quota
          t.string :fds_quota
      end
  end
end
