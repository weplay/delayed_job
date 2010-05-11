require 'rubygems'
require 'optparse'
require 'active_record'

options = { :environment => (ENV['RAILS_ENV'] || "development").dup }
code = nil

ARGV.clone.options do |opts|
  script_name = File.basename($0)
  opts.banner = "Usage: #{$0} [options] ('Some.ruby(code)')"

  opts.separator ""

  opts.on("-e", "--environment=name", String,
          "Specifies the environment for the runner to operate under (test/development/production).",
          "Default: development") { |v| options[:environment] = v }

  opts.separator ""

  opts.on("-h", "--help",
          "Show this help message.") { $stderr.puts opts; exit }

  opts.separator ""

  opts.on("-p", "--priority=N", Float, "Priority of Job to be enqueued") do |n|
    options[:priority] = n
  end

  opts.on("-q", "--quiet",
          "No logging.") { options[:quiet] = true }

  opts.order! { |o| code ||= o } rescue retry
end

ARGV.delete(code)

db = options[:environment]
configurations = YAML.load_file(File.join(File.dirname(__FILE__), '../../../../../../config', 'database.yml'))
raise "no configuration for '#{db}'" unless configurations.key? db
configuration = configurations[db]
ActiveRecord::Base.logger = Logger.new(STDOUT) unless options[:quiet]
ActiveRecord::Base.establish_connection(configuration)

if code.nil?
  $stderr.puts "Run '#{$0} -h' for help."
  exit 1
else
  require "delayed_job"
  priority = options[:priority] || 0
  Delayed::Job.enqueue Delayed::EvaledJob.new { code }, priority
end
