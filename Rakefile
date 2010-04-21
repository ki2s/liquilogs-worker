
require 'lib/liquilogs'

LiquiLogs::Worker.rake_tasks.each do |taskname, methodname,  description|
  desc description
  task taskname do
    LiquiLogs::Worker.create.send methodname
  end
end


# namespace :data do

#   desc 'fetch data.tgz from bucket and prepare them'
#   task :fetch do
#     LiquiLogs::Worker.create.fetch_data
#   end

#   desc 'pack data files into tgz and push them to the bucket archive'
#   task :store do
#     LiquiLogs::Worker.create.store_data
#   end

# end

# namespace :logs do
#   desc 'fetch log files'
#   task :fetch do
#     LiquiLogs::Worker.create.fetch_logs
#   end

#   desc 'pack logs, push them to the archive and delete old logs'
#   task :store do
#     LiquiLogs::Worker.create.store_logs
#   end

# end

# namespace :stats do

#   desc 'create conf file'
#   task :create_config do
#     LiquiLogs::Worker.create.create_config
#   end

#   desc 'run awstats'
#   task :run do
#     LiquiLogs::Worker.create.run_stats
#   end

# end


# namespace :pages do

#   desc 'create HTML pages'
#   task :create do
#     LiquiLogs::Worker.create.create_pages
#   end

#   desc 'store HTML pages to S3'
#   task :store do
#     LiquiLogs::Worker.create.store_pages
#   end

# end


# desc 'run data cycle'
# task :run do
#   LiquiLogs::Worker.create.run
# end
