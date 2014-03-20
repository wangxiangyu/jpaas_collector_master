require "config"
require "nats"

module CollectorMaster
    class CollectorMaster
        def initialize(config_path)
            @config=Config.new(config_path).config
            @nats=nil
            @collectors={}
            @tasks={}
        end
        attr_reader :config
        attr_reader :nats
        def run
            init_task
            register_collector
            register_task
        end
        def setup_nats
            @nats = Nats.new(config['message_bus_uri'])
        end
        def init_task
            (0..255).each do |index|
                @tasks[index]=Time.now.to_i
            end
        end
        def assign_task
            collector_num=@collectors.size
            task_for_each_collector=512/collector_num+1
            count=0
            collector_ips=@collector.keys.sort
            collector_ips.each do |ip|
                collector_client=@collectors[ip]
                data={}
                collector_client.start=data["start"]=task_for_each_collector*count
                collector_client.end=data["end"]=(task_for_each_collector*(count+1)-1)
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
            @collectors[ip].update_time=Time.now.to_i
        end
        def timeout?(time)
            Time.now.to_i-time.to_i>5
        end
        def remove_dead_collector
            EM.add_periodic_timer(10) do
                @collectors.each do |ip,collector_client|
                    if timeout?(collector_client.update_time)
                        delete_collector(ip)
                    end
                end
            end
        end
        def register_collector
            nats.subscribe("collector_register") do |message|
                ip=message.data["ip"]
                if @collectors.hash_key?(ip)
                    add_collector(ip)
                else
                    update_collector(ip)
                end
            end
        end
        
        def check_task_assign
            EM.add_periodic_timer(10) do
                all_assign=true
                (0.255).each do |index|
                   all_assign=false if @tasks[index].to_i < Time.now.to_i-10
                end
                assign_task unless all_assign           
            end
        end

        def register_task
            nats.subscribe("task_register") do |message|
                index=message.data["index"]
                @tasks[index]=Time.now.to_i
            end
        end
        class CollectorClient
            def initialize(ip)
                @update_time=Time.now.to_i
                @start=nil
                @end=nil
                @ip=ip
            end
            attr_accessor :update_time, :start, :end, :ip 
        end
    end
end
