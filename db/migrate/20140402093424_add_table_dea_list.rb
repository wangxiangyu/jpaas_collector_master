class AddTableDeaList < ActiveRecord::Migration
    def change
     create_table :dea_list do |t|
         t.string :uuid
         t.string :ip
         t.string :cluster_num
         t.timestamps
     end
 end
end
