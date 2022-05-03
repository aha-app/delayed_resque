require 'active_record'

module DelayedResque
  class PerformableMethod < Struct.new(:object, :method, :args)
    include UniqueJobs

    CLASS_STRING_FORMAT = /^CLASS\:([A-Z][\w\:]+)$/
    AR_STRING_FORMAT = /^AR\:([A-Z][\w\:]+)\:(\d+)$/

    def initialize(object, method, options, args)
      raise NoMethodError, "undefined method `#{method}' for #{object.inspect}" unless object.respond_to?(method)

      @object = dump(object)
      @method = method.to_sym
      @options = options
      @args = args.map { |a| dump(a) }
    end

    def display_name
      case self.object
      when CLASS_STRING_FORMAT then "#{$1}.#{method}"
      when AR_STRING_FORMAT then "#{$1}##{method}"
      else "Unknown##{method}"
      end
    end

    def queue
      @options[:queue] || self.class.queue
    end

    def self.queue
      @queue || "default"
    end

    def self.with_queue(queue)
      old_queue = @queue
      @queue = queue
      yield
    ensure
      @queue = old_queue
    end

    def self.around_perform_with_unique(options)
      if unique_job?(options)
        unless last_unique_job?(options)
          ::Rails.logger.info("Ignoring duplicate copy of unique job. #{UNIQUE_JOB_ID}: #{options[UNIQUE_JOB_ID]}")
          return
        end

        untrack_unique_job(options)
      end

      yield options
    end

    def self.perform(options)
      object = options["obj"]
      method = options["method"]
      args = options["args"]
      arg_objects = []
      loaded_object =
        begin
          arg_objects = args.map{|a| self.load(a)}
          self.load(object)
        rescue ActiveRecord::RecordNotFound
          Rails.logger.warn("PerformableMethod: failed to find record for #{object.inspect}")
          # We cannot do anything about objects which were deleted in the meantime
          return true
        end
      loaded_object.send(method, *arg_objects)
    end

    def store
      hsh = {"obj" => @object, "method" => @method, "args" => @args}.merge(@options[:params] || {})
      unless @options[:unique] || @options[:throttle] || @options[:at] || @options[:in]
        hsh["t"] = Time.now.to_f
      end

      hsh[UNIQUE_JOB_ID] = unique_job_id if @options[:unique]

      hsh
    end

    private

    def self.load(arg)
      case arg
      when CLASS_STRING_FORMAT then $1.constantize
      when AR_STRING_FORMAT then $1.constantize.find($2)
      else arg
      end
    end

    def dump(arg)
      case arg
      when Class, Module then class_to_string(arg)
      when ActiveRecord::Base then ar_to_string(arg)
      else arg
      end
    end

    def ar_to_string(obj)
      "AR:#{obj.class.base_class.name}:#{obj.id}"
    end

    def class_to_string(obj)
      "CLASS:#{obj.name}"
    end
  end
end
