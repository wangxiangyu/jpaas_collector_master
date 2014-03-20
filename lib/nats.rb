# coding: UTF-8

require "steno"
require "steno/core_ext"
require "nats/client"

module CollectorMaster
  class Nats

    attr_reader :nats_uri
    def initialize(nats_uri)
      @nats_uri = nats_uri
      @client    = nil
    end

    def stop(sids)
      sids.each { |_, sid| client.unsubscribe(sid) }
      sids = {}
    end

    def publish(subject, data)
      client.publish(subject, Yajl::Encoder.encode(data))
    end

    def request(subject, data = {})
      client.request(subject, Yajl::Encoder.encode(data)) do |raw_data, respond_to|
        begin
          yield handle_incoming_message("response to #{subject}", raw_data, respond_to)
        rescue => e
   #       logger.error "Error \"#{e}\" raised while processing #{subject.inspect}: #{raw_data}"
           p "Error \"#{e}\" raised while processing #{subject.inspect}: #{raw_data}"
        end
      end
    end

    def subscribe(subject, opts={})
      sid = client.subscribe(subject, opts) do |raw_data, respond_to|
        begin
          yield handle_incoming_message(subject, raw_data, respond_to)
        rescue => e
          #logger.error "Error \"#{e}\" raised while processing #{subject.inspect}: #{raw_data}"
          p "Error \"#{e}\" raised while processing #{subject.inspect}: #{raw_data}"
        end
      end
      sid
    end

    def unsubscribe(sid)
      client.unsubscribe(sid)
    end

    def client
      @client ||= create_nats_client
    end

    def create_nats_client
      #logger.info "Connecting to NATS on #{config["nats_uri"]}"
      p  "Connecting to NATS on #{nats_uri}"
      # NATS waits by default for 2s before attempting to reconnect, so a million reconnect attempts would
      # save us from a NATS outage for approximately 23 days - which is large enough.
      ::NATS.connect(:uri => nats_uri, :max_reconnect_attempts => 999999)
    end

    class Message
      def self.decode(nats, subject, raw_data, respond_to)
        data = Yajl::Parser.parse(raw_data)
        new(nats, subject, data, respond_to)
      end

      attr_reader :nats
      attr_reader :subject
      attr_reader :data
      attr_reader :respond_to

      def initialize(nats, subject, data, respond_to)
        @nats       = nats
        @subject    = subject
        @data       = data
        @respond_to = respond_to
      end

      def respond(data)
        message = response(data)
        message.publish
      end

      def response(data)
        self.class.new(nats, respond_to, data, nil)
      end

      def publish
        nats.publish(subject, data)
      end
    end

    private

    def handle_incoming_message(subject, raw_data, respond_to)
      message = Message.decode(self, subject, raw_data, respond_to)
      #logger.debug "Received on #{subject.inspect}: #{message.data.inspect}"
      p "Received on #{subject.inspect}: #{message.data.inspect}"
      message
    end
  end
end
