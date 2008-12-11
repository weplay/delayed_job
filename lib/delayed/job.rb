
module Delayed

  class DeserializationError < StandardError
  end

  class Job < ActiveRecord::Base
    MAX_ATTEMPTS = 25
    set_table_name :delayed_jobs

    cattr_accessor :worker_name, :min_priority, :max_priority
    self.worker_name = "pid:#{Process.pid}"      
    self.min_priority = nil
    self.max_priority = nil
  
    NextTaskSQL         = '(`locked_by` = ?) OR (`run_at` <= ? AND (`locked_at` IS NULL OR `locked_at` < ?))'
    NextTaskOrder       = 'priority DESC, run_at ASC'
    ParseObjectFromYaml = /\!ruby\/\w+\:([^\s]+)/

    class LockError < StandardError
    end

    def self.clear_locks!
      connection.execute "UPDATE #{table_name} SET `locked_by`=NULL, `locked_at`=NULL WHERE `locked_by`=#{quote_value(worker_name)}"
    end

    def payload_object
      @payload_object ||= deserialize(self['handler'])
    end                 
    
    def name
      text = handler.gsub(/\n/, ' ')
      "#{id} (#{text.length > 40 ? "#{text[0..40]}..." : text})" 
    end

    def payload_object=(object)
      self['handler'] = object.to_yaml
    end

    def reschedule(message, time = nil)
      if self.attempts < MAX_ATTEMPTS
        time ||= Job.db_time_now + (attempts ** 4) + 5

        self.attempts    += 1
        self.run_at       = time
        self.last_error   = message  
        self.unlock
        save!
      else    
        logger.info "* [JOB] PERMANENTLY removing #{self.name} because of #{attempts} consequetive failures."
        destroy
      end
    end                                  

    def self.enqueue(object, priority = 0)
      unless object.respond_to?(:perform)
        raise ArgumentError, 'Cannot enqueue items which do not respond to perform'
      end

      Job.create(:payload_object => object, :priority => priority.to_i)
    end

    def self.find_available(limit = 5)
      
      time_now = db_time_now          
      
      sql = NextTaskSQL.dup
      conditions = [time_now, time_now, worker_name]
      
      if self.min_priority
        sql << ' AND (`priority` >= ?)'
        conditions << min_priority
      end
      
      if self.max_priority
        sql << ' AND (`priority` <= ?)'
        conditions << max_priority         
      end
           
      conditions.unshift(sql)         
           
      ActiveRecord::Base.silence do
        find(:all, :conditions => conditions, :order => NextTaskOrder, :limit => limit)
      end
    end                                    
        

    # Get the payload of the next job we can get an exclusive lock on. 
    # If no jobs are left we return nil
    def self.reserve(max_run_time = 4.hours)                                                                                 
                    
      # We get up to 5 jobs from the db. In face we cannot get exclusive access to a job we try the next. 
      # this leads to a more even distribution of jobs across the worker processes 
      find_available(5).each do |job|                       
        begin                                              
          logger.info "* [JOB] aquiring lock on #{job.name}"
          job.lock_exclusively!(max_run_time, worker_name)
          runtime =  Benchmark.realtime do 
            yield job.payload_object 
            job.destroy
          end
          logger.info "* [JOB] #{job.name} completed after %.4f" % runtime
          
          return job                                     
        rescue LockError
          # We did not get the lock, some other worker process must have
          logger.warn "* [JOB] failed to aquire exclusive lock for #{job.name}"
        rescue StandardError => e
          job.reschedule e.to_s
          log_exception(job, e)
          return job
        end
      end

      nil
    end

    # This method is used internally by reserve method to ensure exclusive access
    # to the given job. It will rise a LockError if it cannot get this lock. 
    def lock_exclusively!(max_run_time, worker = worker_name)
      now = self.class.db_time_now
      
      affected_rows = if locked_by != worker 

        # We don't own this job so we will update the locked_by name and the locked_at
        connection.update(<<-end_sql, "#{self.class.name} Update to aquire exclusive lock")
          UPDATE #{self.class.table_name}
          SET `locked_at`=#{quote_value(now)}, `locked_by`=#{quote_value(worker)} 
          WHERE #{self.class.primary_key} = #{quote_value(id)} AND (`locked_at` IS NULL OR `locked_at` < #{quote_value(now - max_run_time.to_i)})
        end_sql

      else

        # We already own this job, this may happen if the job queue crashes. 
        # Simply resume and update the locked_at
        connection.update(<<-end_sql, "#{self.class.name} Update exclusive lock")
          UPDATE #{self.class.table_name}
          SET `locked_at`=#{quote_value(now)} 
          WHERE #{self.class.primary_key} = #{quote_value(id)} AND (`locked_by`=#{quote_value(worker)})
        end_sql

      end

      unless affected_rows == 1
        raise LockError, "Attempted to aquire exclusive lock failed"
      end

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
            j.perform
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
      attempt_to_load_file = true

      begin
        handler = YAML.load(source) rescue nil
        return handler if handler.respond_to?(:perform)

        if handler.nil?
          if source =~ ParseObjectFromYaml

            # Constantize the object so that ActiveSupport can attempt 
            # its auto loading magic. Will raise LoadError if not successful.
            attempt_to_load($1)

            # If successful, retry the yaml.load
            handler = YAML.load(source)
            return handler if handler.respond_to?(:perform)
          end
        end

        if handler.is_a?(YAML::Object)

          # Constantize the object so that ActiveSupport can attempt 
          # its auto loading magic. Will raise LoadError if not successful.
          attempt_to_load(handler.class)

          # If successful, retry the yaml.load
          handler = YAML.load(source)
          return handler if handler.respond_to?(:perform)  
        end

        raise DeserializationError, 'Job failed to load: Unknown handler. Try to manually require the appropiate file.'

      rescue TypeError, LoadError, NameError => e 

        raise DeserializationError, "Job failed to load: #{e.message}. Try to manually require the required file."
      end
    end

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
end