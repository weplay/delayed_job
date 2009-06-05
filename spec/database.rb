$:.unshift(File.dirname(__FILE__) + '/../lib')
$:.unshift(File.dirname(__FILE__) + '/../../rspec/lib')

require 'rubygems'
require 'active_record'
gem 'sqlite3-ruby'

require File.dirname(__FILE__) + '/../init'
require 'spec'

ActiveRecord::Base.logger = Logger.new('/tmp/dj.log')
ActiveRecord::Base.establish_connection({
  :adapter  => 'mysql',
  :username => 'root',
  :encoding => 'utf8',
  :database => 'delayed_job_test',
})
ActiveRecord::Migration.verbose = false

sql = <<-SQL
  DROP TABLE IF EXISTS `delayed_jobs`;
  CREATE TABLE `delayed_jobs` (
    `id` INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `priority`   TINYINT NOT NULL DEFAULT 0,
    `attempts`   TINYINT NOT NULL DEFAULT 0,
    `handler`    TEXT,
    `run_at`     DATETIME,
    `locked_at`  DATETIME,
    `locked_by`  CHAR(20),
    `created_at` DATETIME
  );

  DROP TABLE IF EXISTS `delayed_job_errors`;
  CREATE TABLE `delayed_job_errors` (
    `id` INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `job_id` INTEGER NOT NULL,
    `message` MEDIUMTEXT NOT NULL,
    `created_at` DATETIME,
    `updated_at` DATETIME
  );

  DROP TABLE IF EXISTS `stories`;
  CREATE TABLE `stories` (
    `id` INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
    `text` VARCHAR(255)
  );
SQL

sql.split(/;/).select(&:present?).each do |sql_statement|
  ActiveRecord::Base.connection.execute sql_statement
end

# Purely useful for test cases...
class Story < ActiveRecord::Base
  def tell; text; end
end
