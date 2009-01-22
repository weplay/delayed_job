class DelayedJobGenerator < Rails::Generator::Base
  
  def manifest
    record do |m|
      m.template 'enqueue_job', 'script/enqueue_job', :chmod => 0755
    end
  end
  
end