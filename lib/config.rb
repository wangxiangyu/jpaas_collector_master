require "membrane"
require "yaml"

module CollectorMaster
    class Config
        attr_reader :config
        DEFAULT_CONFIG = {}
        def self.schema
            ::Membrane::SchemaParser.parse do
            {
              "message_bus_uri" => String,
               "logging" => {
                  "file" => String,
              },
            }
            end
        end
        def initialize(file_path)
            @config = DEFAULT_CONFIG.merge(YAML.load_file(file_path))
            validate
        end
        def validate
            self.class.schema.validate(@config)
        end
    end
end
