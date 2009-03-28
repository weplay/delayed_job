module Delayed
  module MessageSending
    def send_later_prioritized(priority, method, *args)
      Delayed::Job.enqueue Delayed::PerformableMethod.new(self, method.to_sym, args), priority.to_i
    end
    
    def send_later(method, *args)
      Delayed::Job.enqueue Delayed::PerformableMethod.new(self, method.to_sym, args)
    end
    
    def send_after(time, method, *args)
      Delayed::Job.enqueue(Delayed::PerformableMethod.new(self, method.to_sym, args), 0, time.from_now)
    end
  end
end