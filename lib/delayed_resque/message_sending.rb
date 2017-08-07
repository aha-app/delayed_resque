require 'active_support'

module DelayedResque
  class DelayProxy < ActiveSupport::ProxyObject
    TRACKED_QUEUE_NAME = "trackedTasks"
    TRACKED_QUEUE_KEY = "tracked_task_key"
    def initialize(payload_class, target, options)
      @payload_class = payload_class
      @target = target
      @options = {:queue => "default"}.update(options)
    end

    def self.tracked_task?(key)
      ::Resque.redis.sismember(TRACKED_QUEUE_NAME, key)
    end

    def self.track_task(key)
      ::Resque.redis.sadd(TRACKED_QUEUE_NAME, key)
    end

    def self.untrack_task(key)
      ::Resque.redis.srem(TRACKED_QUEUE_NAME, key)
    end

    def self.args_tracking_key(args)
      args_hash = Array(args).first
      args_hash[TRACKED_QUEUE_KEY].presence if args_hash.is_a?(::Hash)
    end

    def method_missing(method, *args)
      queue = @options[:queue] || @payload_class.queue
      performable = @payload_class.new(@target, method.to_sym, @options, args)
      stored_options = performable.store

      if @options[:tracked].present?
        ::DelayedResque::DelayProxy.track_task(@options[:tracked])
        stored_options[TRACKED_QUEUE_KEY] = @options[:tracked]
      end

      if @options[:unique]
        if @options[:at] || @options[:in]
          ::Resque.remove_delayed(@payload_class, stored_options)
        else
          ::Resque.dequeue(@payload_class, stored_options)
        end
      end

      ::Rails.logger.warn("Queuing for RESQUE: #{stored_options['method']}: #{stored_options.inspect}")

      if @options[:at]
        ::Resque.enqueue_at(@options[:at], @payload_class, stored_options)
      elsif @options[:in]
        ::Resque.enqueue_in(@options[:in], @payload_class, stored_options)
      else
        ::Resque.enqueue_to(queue, @payload_class, stored_options)
      end
    end
  end


  module MessageSending
    def delay(options = {})
      DelayProxy.new(PerformableMethod, self, options)
    end
    alias __delay__ delay
  end
end
