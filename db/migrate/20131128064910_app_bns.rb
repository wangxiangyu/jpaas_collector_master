class AppBns < ActiveRecord::Migration
   def change
        create_table :app_bns do |t|
            t.string :name
            t.string :organization
            t.string :space
            t.string :app_name
            t.string :app_key
            t.timestamps
        end
    end
end
