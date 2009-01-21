# Re-definitions are appended to existing tasks
task :environment
task :merb_env

namespace :jobs do
  desc "Clear the delayed_job queue."
  task :clear => [:merb_env, :environment] do
    Delayed::Job.delete_all
  end

  desc "Start a delayed_job worker."
  task :work => [:merb_env, :environment] do
    Delayed::Worker.new(:min_priority => ENV['MIN_PRIORITY'], :max_priority => ENV['MAX_PRIORITY']).start
  end
  
  desc "Enqueue a delayed_job by inserting into the jobs DB table without loading your entire Rails environment."
  task :enqueue do
    ENV['RAILS_ENV'] ||= 'development'
    raise 'Usage: rake jobs:add METHOD=SomeClass.do_something RAILS_ENV=development' unless ENV['METHOD']
    
    $LOAD_PATH.unshift(File.expand_path(File.dirname(__FILE__) + "/../lib"))
    require "delayed_job"
    
    db = ENV['RAILS_ENV']
    configurations = YAML.load_file(File.join(File.dirname(__FILE__), '../../../../config', 'database.yml'))
    raise "no configuration for '#{db}'" unless configurations.key? db
    configuration = configurations[db]
    ActiveRecord::Base.logger = Logger.new(STDOUT)
    ActiveRecord::Base.establish_connection(configuration)

    Delayed::Job.enqueue Delayed::EvaledJob.new { ENV['METHOD'] }
  end
  
end
