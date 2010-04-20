
require 'lib/liquilogs'
ll_object = LiquiLogs::Worker.create


namespace :data do

  desc 'fetch data.tgz from bucket and prepare them'
  task :fetch do
    ll_object.fetch_data
  end

  desc 'pack data files into tgz and push them to the bucket archive'
  task :store do
    ll_object.store_data
  end

end

namespace :logs do
  desc 'fetch log files'
  task :fetch do
    ll_object.fetch_logs
  end

  desc 'pack logs, push them to the archive and delete old logs'
  task :store do
    ll_object.store_logs
  end

end

namespace :stats do

  desc 'create conf file'
  task :create_config do
    ll_object.create_config
  end

  desc 'run awstats'
  task :run do
    ll_object.run_stats
  end

end


namespace :pages do

  desc 'create HTML pages'
  task :create do
    ll_object.create_pages
  end

  desc 'store HTML pages to S3'
  task :store do
    ll_object.store_pages
  end

end


desc 'run data cycle'
task :run, [:sitename, :bucket, :log_prefix, :conf_type] do |t, args|
  ll_object.run
end
