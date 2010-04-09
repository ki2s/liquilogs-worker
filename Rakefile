
require 'pathname'

# if File.exists? 'config.rb'
#   load 'config.rb'
# else
  require 'ostruct'
  config = OpenStruct.new
# end

require 'lib/liquilogs'
ll_object = LiquiLogs::Worker.create

require 'aws/s3'
AWS::S3::Base.establish_connection!(
  :access_key_id     => "AKIAIRHLSVBIOYHXVQTQ",
  :secret_access_key => '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'
)



namespace :data do

  desc 'fetch data.tgz from bucket and prepare them'
  task :fetch do
    ll_object.fetch_data
  end

  desc 'pack data files into tgz and push them to the bucket archive'
  task :store do
    ll_object.store_data
  end

  desc 'clean up local data dir'
  task :clean, :sitename do |t, args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    rm_rf Dir["#{sitename}/data/*"]
  end

end

namespace :logs do
  desc 'fetch log files'
  task :fetch do
    ll_object.fetch_logs
  end

  desc 'pack logs, push them to the archive and delete old logs'
  task :store, [:sitename, :bucket, :log_prefix] do |t,args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?
    bucket = args.bucket || config.bucket
    raise 'no bucket given' if bucket.nil?
    log_prefix = args.log_prefix || config.log_prefix
    raise 'no log_prefix given' if log_prefix.nil?

    log_files = Dir["#{sitename}/logs/*"].map{|f| File.split(f).last}
    now = Time.now
#    store_url = "#{ll}/#{now.strftime('%Y/%m')}/#{sitename}-logs-#{now.strftime('%F-%H-%M-%S.tgz')}"
    store_url = "liquilogs/#{now.strftime('%Y/%m')}/#{sitename}-logs-#{now.strftime('%F-%H-%M-%S.tgz')}"

    puts( "tar c -C #{sitename}/logs #{log_files.join(' ')} | gzip -9fc")
    IO.popen( "tar c -C #{sitename}/logs #{log_files.join(' ')} | gzip -9fc") do |io|
    puts "storing #{store_url}"
      AWS::S3::S3Object.store( store_url,
			       io.read,
                               bucket,
                               :access => :public_read )
    end

    log_files.each do |log|
      # TODO: better deletion
      puts "deleting #{log_prefix[0..log_prefix.rindex( '/')] + log}"
#      AWS::S3::S3Object.delete( log_prefix[0..log_prefix.rindex('/')] + log, bucket )
    end

    Rake::Task["logs:clean"].invoke sitename
  end

  desc 'clean up local logs dir'
  task :clean, :sitename do |t, args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    rm_rf Dir["#{sitename}/logs/*"]
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
  task :create, :sitename do |t,args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    mkpath "#{sitename}/html" unless File.exists? "#{sitename}/html"

    ENV['AWSTATS_PATH']= Pathname.pwd
    ENV['AWSTATS_SITEDOMAIN']= sitename

    system( "awstats/tools/awstats_buildstaticpages.pl -config=#{sitename} -awstatsprog=#{Pathname.pwd + 'awstats' + 'wwwroot' + 'cgi-bin' + 'awstats.pl'} -dir=#{Pathname.pwd + sitename + 'html'} -diricons=http://ki2s-icons.s3.amazonaws.com/6.95/icon" )

    ENV.delete('AWSTATS_SITEDOMAIN')
    ENV.delete('AWSTATS_PATH')
  end

  desc 'store HTML pages to S3'
  task :store, [:sitename, :bucket] do |t,args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?
    bucket = args.bucket || config.bucket
    raise 'no bucket given' if bucket.nil?

    Dir["#{sitename}/html/*"].each do |f_name|
      puts "sending #{f_name}"
      AWS::S3::S3Object.store( f_name, open( f_name ), bucket, :access => :public_read )
    end
  end

  desc 'clean up local HTML'
  task :clean, :sitename do |t, args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    rm_rf Dir["#{sitename}/html/*"]
  end

end


desc 'run data cycle'
task :run, [:sitename, :bucket, :log_prefix, :conf_type] do |t, args|
  Rake::Task["data:fetch"].invoke args.sitename, args.bucket
  Rake::Task["logs:fetch"].invoke args.sitename, args.bucket, args.log_prefix
  Rake::Task["stats:create_config"].invoke args.sitename, args.conf_type
  Rake::Task["stats:run"].invoke args.sitename
  Rake::Task["data:store"].invoke args.sitename, args.bucket
#  Rake::Task["logs:store"].invoke args.sitename, args.bucket, args.log_prefix
  Rake::Task["pages:create"].invoke args.sitename
  Rake::Task["pages:store"].invoke args.sitename, args.bucket
end

desc 'clean everything up'
task :clean, :sitename do |t, args|
  sitename = args.sitename || config.sitename
  raise 'no sitename given' if sitename.nil?

  rm_rf Dir["awstats/wwwroot/cgi-bin/awstats.#{sitename}.conf"]
  Rake::Task["data:clean"].invoke sitename
  Rake::Task["logs:clean"].invoke sitename
  Rake::Task["pages:clean"].invoke sitename
end

desc 'show config'
task :show_config do
  puts "show_config in #{Dir.pwd}"
end
