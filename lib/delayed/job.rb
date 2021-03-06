require "socket"

module Delayed

  class DeserializationError < StandardError
  end

  class SerializationError < StandardError
  end

  class JobError < ActiveRecord::Base
    set_table_name :delayed_job_errors
  end

  class Job < ActiveRecord::Base
    MAX_ATTEMPTS = 25
    MAX_RUN_TIME = 4.hours
    set_table_name :delayed_jobs

    has_one :last_error, :class_name => "Delayed::JobError", :order => "delayed_job_errors.id DESC"
    has_many :errors, :class_name => "Delayed::JobError"

    # Every worker has a unique name which by default is the pid of the process.
    # There are some advantages to overriding this with something which survives worker retarts:
    # Workers can safely resume working on tasks which are locked by themselves. The worker will assume that it crashed before.
    cattr_accessor :worker_name
    self.worker_name = [Process.pid.to_s, Socket.gethostname].join(":")

    NextTaskSQL         = '(run_at <= ? AND (locked_at IS NULL OR locked_at < ?) OR (locked_by = ?))'
    NextTaskOrder       = 'priority DESC, run_at ASC'

    ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

    cattr_accessor :min_priority, :max_priority
    self.min_priority = nil
    self.max_priority = nil

    class LockError < StandardError
    end

    class << self
      attr_accessor :active_id
    end

    def self.clear_locks!
      update_all("locked_by = null, locked_at = null", ["locked_by = ?", worker_name])
    end

    def last_error_message
      last_error && last_error.message
    end

    def payload_object
      @payload_object ||= deserialize(self['handler'])
    end

    def name
      @name ||= begin
        payload = payload_object
        if payload.respond_to?(:display_name)
          payload.display_name
        else
          payload.class.name
        end
      end
    end

    def payload_object=(object)
      yml = object.to_yaml
      # Fail fast if the Job would be cut off by the limit of a MySQL MEDIUMTEXT
      raise SerializationError unless yml.size < 16_777_215
      self['handler'] = object.to_yaml
    end

    def reschedule(message, backtrace = [], time = nil)
      if self.attempts < MAX_ATTEMPTS
        time ||= Job.db_time_now + (attempts ** 4) + 5

        self.attempts    += 1
        self.run_at       = time
        self.unlock
        save!

        JobError.create!(:job_id => self.id, :message => (message + "\n" + backtrace.join("\n")))
      else
        logger.info "* [JOB] PERMANENTLY removing #{self.name} because of #{attempts} consecutive failures."
        destroy
      end
    end

    def self.enqueue(*args, &block)
      object = block_given? ? EvaledJob.new(&block) : args.shift

      unless object.respond_to?(:perform) || block_given?
        raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
      end

      priority = args.first || 0
      run_at   = args[1]

      Job.create(:payload_object => object, :priority => priority.to_i, :run_at => run_at)
    end

    def self.find_available(limit = 5, max_run_time = MAX_RUN_TIME)

      time_now = db_time_now

      sql = NextTaskSQL.dup

      conditions = [time_now, time_now - max_run_time, worker_name]

      if self.min_priority
        sql << ' AND (priority >= ?)'
        conditions << min_priority
      end

      if self.max_priority
        sql << ' AND (priority <= ?)'
        conditions << max_priority
      end

      conditions.unshift(sql)

      records = ActiveRecord::Base.silence do
        find(:all, :conditions => conditions, :order => NextTaskOrder, :limit => limit)
      end

      # Commenting out until I understand the motivation -LM
      #records.sort_by { rand() }
    end

    # Get the payload of the next job we can get an exclusive lock on.
    # If no jobs are left we return nil
    def self.reserve(max_run_time = MAX_RUN_TIME, &block)

      # We get up to 5 jobs from the db. In face we cannot get exclusive access to a job we try the next.
      # this leads to a more even distribution of jobs across the worker processes
      find_available(5, max_run_time).each do |job|
        begin
          logger.info "* [JOB] acquiring lock on #{job.name}"
          job.lock_exclusively!(max_run_time, worker_name)
          runtime =  Benchmark.realtime do
            begin
              Delayed::Job.active_id = job.id
              job.invoke_job(&block)
            ensure
              Delayed::Job.active_id = nil
            end

            job.destroy
          end
          logger.info "* [JOB] #{job.name} completed after %.4f" % runtime

          return job
        rescue LockError
          # We did not get the lock, some other worker process must have
          logger.warn "* [JOB] failed to acquire exclusive lock for #{job.name}"
        rescue StandardError => e
          job.reschedule e.message, e.backtrace
          log_exception(job, e)
          return job
        end
      end

      nil
    end

    # Moved into its own method so that new_relic can trace it.
    def invoke_job(&block)
      block.call(payload_object)
    end

    # This method is used internally by reserve method to ensure exclusive access
    # to the given job. It will rise a LockError if it cannot get this lock.
    def lock_exclusively!(max_run_time, worker = worker_name)
      now = self.class.db_time_now
      affected_rows = if locked_by != worker
        # We don't own this job so we will update the locked_by name and the locked_at
        self.class.update_all(["locked_at = ?, locked_by = ?", now, worker], ["id = ? and (locked_at is null or locked_at < ?)", id, (now - max_run_time.to_i)])
      else
        # We already own this job, this may happen if the job queue crashes.
        # Simply resume and update the locked_at
        self.class.update_all(["locked_at = ?", now], ["id = ? and locked_by = ?", id, worker])
      end
      raise LockError.new("Attempted to acquire exclusive lock failed") unless affected_rows == 1

      self.locked_at    = now
      self.locked_by    = worker
    end

    def unlock
      self.locked_at    = nil
      self.locked_by    = nil
    end

    # This is a good hook if you need to report job processing errors in additional or different ways
    def self.log_exception(job, error)
      logger.error "* [JOB] #{job.name} failed with #{error.class.name}: #{error.message} - #{job.attempts} failed attempts"
      logger.error(error)
    end

    def self.work_off(num = 100)
      success, failure = 0, 0

      num.times do
        job = self.reserve do |j|
          begin
            ActiveRecord::Base.transaction do
              j.perform
            end
            success += 1
          rescue
            failure += 1
            raise
          end
        end

        break if job.nil?
      end

      return [success, failure]
    end

  private

    def deserialize(source)
      handler = YAML.load(source) rescue nil

      unless handler.respond_to?(:perform)
        if handler.nil? && source =~ ParseObjectFromYaml
          handler_class = $1
        end
        attempt_to_load(handler_class || handler.class)
        handler = YAML.load(source)
      end

      return handler if handler.respond_to?(:perform)

      raise DeserializationError,
        'Job failed to load: Unknown handler. Try to manually require the appropiate file.'
    rescue TypeError, LoadError, NameError => e
      raise DeserializationError,
        "Job failed to load: #{e.message}. Try to manually require the required file."
    end

    # Constantize the object so that ActiveSupport can attempt
    # its auto loading magic. Will raise LoadError if not successful.
    def attempt_to_load(klass)
       klass.constantize
    end

    def self.db_time_now
      (ActiveRecord::Base.default_timezone == :utc) ? Time.now.utc : Time.now
    end

  protected

    def before_save
      self.run_at ||= self.class.db_time_now
    end

  end

  class EvaledJob
    def initialize
      @job = yield
    end

    def perform
      eval(@job)
    end
  end
end
