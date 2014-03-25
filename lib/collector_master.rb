require "config"
require "nats"
require "logger"

module CollectorMaster
    class CollectorMaster
        def initialize(config_path)
            @config=Config.new(config_path).config
            @nats=nil
            @collectors={}
            @tasks={}
            @logger=Logger.new(config['logging']['file'])
            @logger.datetime_format = "%Y-%m-%d %H:%M:%S"
            @logger.formatter = proc do |severity, datetime, progname, msg|
                "[#{datetime}] #{severity} : #{msg}\n"
            end
            @logger.level = Logger::DEBUG
        end
        attr_reader :config
        attr_reader :nats
        attr_reader :logger
        def run
            begin
            init_task
            register_collector
            register_task
            remove_dead_collector
            check_task_assign
            rescue => e
                logger.error("Error in collect master: #{e.message} #{e.backtrace}")
            end
        end
        def setup_nats
            @nats = Nats.new(self,config['message_bus_uri'])
        end
        def init_task
            (0..255).each do |index|
                @tasks[index]=Time.now.to_i
            end
        end
        def assign_task
            collector_num=@collectors.size
            return if collector_num==0
            task_for_each_collector=512/collector_num+1
            count=0
            collector_ips=@collectors.keys.sort
            collector_ips.each do |ip|
                collector_client=@collectors[ip]
                data={}
                collector_client.start_index=data["start_index"]=task_for_each_collector*count
                collector_client.end_index=data["end_index"]=(task_for_each_collector*(count+1)-1)
                count=count+1
                nats.publish("collector_task_#{ip}",data)
            end
        end
        def delete_collector(ip)
            @collectors.delete(ip)
            assign_task
        end
        def add_collector(ip)
            @collectors[ip]=CollectorClient.new(ip)
            assign_task
        end
        def update_collector(ip)
            @collectors[ip].update_time=Time.now.to_i if @collectors.has_key?(ip)
        end
        def timeout?(time)
            Time.now.to_i-time.to_i>5
        end
        def remove_dead_collector
            EM::PeriodicTimer.new(10) do
                @collectors.each do |ip,collector_client|
                    if timeout?(collector_client.update_time)
                        logger.info("remove dead collector #{ip}")
                        delete_collector(ip)
                    end
                end
            end
        end
        def register_collector
            nats.subscribe("collector_register") do |message|
                ip=message.data["ip"]
                if !@collectors.has_key?(ip)
                    add_collector(ip)
                else
                    update_collector(ip)
                end
            end
        end
        
        def check_task_assign
            EM::PeriodicTimer.new(10) do
                all_assign=true
                (0..255).each do |index|
                   all_assign=false if @tasks[index].to_i < Time.now.to_i-10
                end
                unless all_assign
                    logger.info("task re assign")
                    assign_task unless all_assign           
                end
            end
        end

        def register_task
            nats.subscribe("task_register") do |message|
                index=message.data["index"]
                index.each do |i|
                    @tasks[i]=Time.now.to_i
                end
            end
        end
        class CollectorClient
            def initialize(ip)
                @update_time=Time.now.to_i
                @start_index=nil
                @end_index=nil
                @ip=ip
            end
            attr_accessor :update_time, :start_index, :end_index, :ip 
        end
    end
end
