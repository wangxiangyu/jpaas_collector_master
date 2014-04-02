class AddInstanceResourceUsageToInstanceStatus < ActiveRecord::Migration
  def change
      change_table :instance_status do |t|
          t.string :cpu_usage
          t.string :mem_usage
          t.string :fds_usage
      end
  end
end
