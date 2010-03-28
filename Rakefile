
require 'pathname'

# if File.exists? 'config.rb'
#   load 'config.rb'
# else
  require 'ostruct'
  config = OpenStruct.new
# end

require 'aws/s3'
AWS::S3::Base.establish_connection!(
  :access_key_id     => "AKIAIRHLSVBIOYHXVQTQ",
  :secret_access_key => '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'
)

ll = "liquilogs"

namespace :dirs do

  desc 'prepare dirs'
  task :prepare, :sitename do |t, args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    %w(logs data html).each { |dir| mkpath "#{sitename}/#{dir}" }
  end
end

namespace :config do

  desc 'create conf file'
  task :create, :sitename do |t, args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    cd Pathname.pwd + 'awstats' + 'wwwroot' + 'cgi-bin', :verbose => false do
#      ln_sf 'awstats.conf.template', "awstats.#{sitename}.conf", :verbose => false
      ln_sf 'awstats.conf.template', "awstats.#{sitename}.conf"
    end
  end
end

namespace :data do

  desc 'fetch data.tgz from bucket and prepare them'
  task :fetch, [:sitename, :bucket] do |t, args|
    # get needed arguments
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?
    bucket = args.bucket || config.bucket
    raise 'no bucket given' if bucket.nil?

    # only fetch data if dir is empty
    if Dir["#{sitename}/data/*"].empty?
      o = AWS::S3::S3Object.find "#{ll}/data.#{sitename}.tgz", args.bucket
      IO.popen("tar zxf - -C #{sitename}/data", 'w') { |p| p.print o.value }
    end
  end

  desc 'pack data files into tgz and push them to the bucket archive'
  task :store, [:sitename, :bucket] do |t, args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?
    bucket = args.bucket || config.bucket
    raise 'no bucket given' if bucket.nil?

    IO.popen( "tar c -C data #{Dir['#{sitename}/data/*'].map{|f| File.split(f).last}.join(' ')} | gzip -9fc" ) do |io|
#      store_url = "#{ll}/data.#{sitename}.#{Time.now.strftime('%Y%m%d-%H%M%S')}.tgz"
      store_url = "#{ll}/data.#{sitename}.tgz"
      puts "storing #{store_url}"
      AWS::S3::S3Object.store( store_url, io.read, bucket, :access => :public_read )
    end

  end

  desc 'clean up local data dir'
  task :clean, :sitename do
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    rm_rf Dir["#{sitename}/data/*"]
  end

end

namespace :logs do
  desc 'fetch log files'
  task :fetch, [:sitename, :bucket, :log_prefix] do |t, args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?
    bucket = args.bucket || config.bucket
    raise 'no bucket given' if bucket.nil?
    log_prefix = args.log_prefix || config.log_prefix
    raise 'no log_prefix given' if log_prefix.nil?

    AWS::S3::Bucket.objects( bucket, :prefix => log_prefix ).each do |s3o|
      open("#{sitename}/logs/#{s3o.key.split('/').last}", 'w') do |file|
        AWS::S3::S3Object.stream(s3o.key, bucket) do |chunk|
          file.write chunk
        end
      end
    end

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
    store_url = "#{ll}/#{now.strftime('%Y/%m')}/#{sitename}-logs-#{now.strftime('%F-%H-%M-%S.tgz')}"

    IO.popen( "tar c -C logs #{log_files.join(' ')} | gzip -9fc") do |io|
    puts "storing #{store_url}"
      AWS::S3::S3Object.store( store_url,
			       io.read,
                               bucket,
                               :access => :public_read )
    end

    log_files.each do |log|
      # TODO: better deletion
      puts "deleting #{log_prefix[0..log_prefix.rindex( '/')] + log}"
      AWS::S3::S3Object.delete( "log-s3/#{log}", bucket )
    end
#    system "for i in `ls -1 logs/`; do s3cmd del #{$from_url}$i; done"

    Rake::Task["logs:clean"].invoke
  end

  desc 'clean up local logs dir'
  task :clean, :sitename do
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    rm_rf Dir["#{sitename}/logs/*"]
  end

end

namespace :stats do

  desc 'run awstats'
  task :run, :sitename do |t,args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    unless Dir["#{sitename}/logs/*"].empty?

      ENV['AWSTATS_PATH']= Pathname.pwd
      ENV['AWSTATS_SITEDOMAIN']= sitename

      system( "awstats/wwwroot/cgi-bin/awstats.pl -config=#{sitename} -update" )

      ENV.delete('AWSTATS_SITEDOMAIN')
      ENV.delete('AWSTATS_PATH')

    end
  end

end


namespace :pages do

  desc 'create HTML pages'
  task :create, :sitename do |t,args|
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

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
  task :clean, :sitename do
    sitename = args.sitename || config.sitename
    raise 'no sitename given' if sitename.nil?

    rm_rf Dir["#{sitename}/html/*"]
  end

end


desc 'prepare everything'
task :prepare, [:sitename, :bucket] do |t, args|
  Rake::Task["dirs:prepare"].invoke args.sitename
  Rake::Task["config:create"].invoke args.sitename
  Rake::Task["data:fetch"].invoke args.sitename, args.bucket
end

desc 'run one cycle'
task :run, [:sitename, :bucket, :log_prefix] do |t, args|
  Rake::Task["logs:fetch"].invoke args.sitename, args.bucket, args.log_prefix
  Rake::Task["stats:run"].invoke args.sitename
  Rake::Task["data:store"].invoke args.sitename, args.bucket
  Rake::Task["logs:store"].invoke args.sitename, args.bucket, args.log_prefix
  Rake::Task["pages:create"].invoke args.sitename
  Rake::Task["pages:store"].invoke args.sitename, args.bucket
end

desc 'clean everything up'
task :clean, :sitename do
  rm_rf Dir["awstats/wwwroot/cgi-bin/awstats.*.conf"]
  Rake::Task["data:clean"].invoke args.sitename
  Rake::Task["logs:clean"].invoke args.sitename
  Rake::Task["pages:clean"].invoke args.sitename
end

desc 'show config'
task :show_config do
  puts "show_config in #{Dir.pwd}"
end
