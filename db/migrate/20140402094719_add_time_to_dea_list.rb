class AddTimeToDeaList < ActiveRecord::Migration
  def change
      change_table :dea_list do |t|
          t.integer :time
      end
  end
end
