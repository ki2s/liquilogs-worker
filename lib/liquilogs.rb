#
#
#

require 'pathname'
require 'fileutils'
require 'rubygems'

#require 'aws/s3'
# AWS::S3::Base.establish_connection!(
#   :access_key_id     => "AKIAIRHLSVBIOYHXVQTQ",
#   :secret_access_key => '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'
# )


module LiquiLogs; end

class LiquiLogs::Worker
  @rake_tasks = []
  class << self; attr_accessor :rake_tasks end

  def initialize config=nil
    require 'ostruct'
    @config= OpenStruct.new(
                            :sitename   => 'rubypulse',
                            :bucket     => 'rubypulse-logs',
                            :log_prefix => 'log-s3/access_log',
                            :conf_type  => 's3',
                            # alex@ki2s.com
                            :aws_key    => 'AKIAIRHLSVBIOYHXVQTQ',
                            :aws_secret => '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'

                            # :sitename   => 'd1l8043zxfup2z.cloudfront.net',
                            # :bucket     => 'rubypulse-logs',
                            # :log_prefix => 'log-cloudfront/',
                            # :conf_type  => 'cloudfront',
                            # # alex@peuchert.de
                            # :aws_key    => 'AKIAIYW27IP7RYPITBCQ',
                            # :aws_secret => '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'
                            )

    require 'aws/s3'
    AWS::S3::Base.establish_connection!( :access_key_id     => @config.aws_key,
                                         :secret_access_key => @config.aws_secret,
                                         :use_ssl           => true,
                                         :persistent        => true
                                         )
  end

  def self.create config=nil
    o = case config
        when Hash
          new config
        end
    return new
  end

  def sitename; @config.sitename; end
  def sitedir; @sitedir ||= Pathname.new( sitename ); end
  def data_dir;
    @data_dir ||= sitedir + 'data'
    @data_dir.mkpath unless @data_dir.exist?
    @data_dir
  end
  def bucket; @config.bucket; end
  def log_prefix; @config.log_prefix; end
  def conf_type; @config.conf_type; end
  def ll; "liquilogs"; end

  @rake_tasks << [[:data,:fetch], :fetch_data, 'fetch data.tgz from bucket and prepare them']
  def fetch_data
    # only fetch data if dir is empty
    if Pathname.glob( data_dir + 'awstats*').empty?
      o = AWS::S3::S3Object.find "#{ll}/data.#{sitename}.tgz", bucket
      IO.popen("tar zxf - -C #{data_dir}", 'w') { |p| p.print o.value }
    end
  end

  @rake_tasks << [[:data,:store], :store_data, 'pack data files into tgz and push them to the bucket archive']
  def store_data
    file_list = Pathname.glob( sitedir+'data'+ '*').map( &:basename).join(' ')
    IO.popen( "tar c -C #{ sitedir+'data' } #{file_list} | gzip -9fc" ) do |io|
#      store_url = "#{ll}/data.#{sitename}.#{Time.now.strftime('%Y%m%d-%H%M%S')}.tgz"
      store_url = "#{ll}/data.#{sitename}.tgz"
      puts "storing #{store_url}"
      AWS::S3::S3Object.store( store_url, io.read, bucket, :access => :public_read )
    end
  end

  @rake_tasks << [[:logs,:fetch], :fetch_logs, 'fetch log files']
  def fetch_logs
    last_log_key_file = data_dir + 'last_log_key'
    last_key = last_log_key_file.exist? ? last_log_key_file.read : ""

#    log_objects = AWS::S3::Bucket.objects( bucket, :prefix => log_prefix, :marker => 'log-s3/access_log-2010-04-22-00-20-42-0E30443645F32174')
    log_objects = AWS::S3::Bucket.objects( bucket, :prefix => log_prefix, :marker => last_key)
    return if log_objects.empty?

    (sitedir + 'logs').open('w') do |file|
      log_objects.each do |s3o|
        puts "fetching #{s3o.key}"
        s3o.value { |chunk| file.write(chunk) }
      end
    end
    (data_dir + 'last_log_key').open('w') do |file|
      file.write log_objects.last.key
    end
  end

  @rake_tasks << [[:logs,:store], :store_logs, 'pack logs, push them to the archive and delete old logs']
  def store_logs
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
  end

  @rake_tasks << [[:stats,:create_config], :create_config, 'create conf file']
  def create_config
    unless File.exists? Pathname.pwd + 'awstats' + 'wwwroot' + 'cgi-bin' + "awstats.#{sitename}.conf"
      cd Pathname.pwd + 'awstats' + 'wwwroot' + 'cgi-bin', :verbose => false do
        FileUtils.ln_sf "awstats.conf.#{conf_type}.template", "awstats.#{sitename}.conf"
      end
    end
  end

  @rake_tasks << [[:stats,:run], :run_stats, 'run awstats']
  def run_stats
    ENV['AWSTATS_PATH']= Pathname.pwd
    ENV['AWSTATS_SITEDOMAIN']= sitename

    system( "awstats/wwwroot/cgi-bin/awstats.pl -config=#{sitename} -update" )

    ENV.delete('AWSTATS_SITEDOMAIN')
    ENV.delete('AWSTATS_PATH')
  end

  @rake_tasks << [[:pages,:create], :create_pages, 'create HTML pages']
  def create_pages
    ( sitedir + 'html' ).mkpath

    ENV['AWSTATS_PATH']= Pathname.pwd
    ENV['AWSTATS_SITEDOMAIN']= sitename

    system( "awstats/tools/awstats_buildstaticpages.pl -config=#{sitename} -awstatsprog=#{Pathname.pwd + 'awstats' + 'wwwroot' + 'cgi-bin' + 'awstats.pl'} -dir=#{Pathname.pwd + sitename + 'html'}" )

    ENV.delete('AWSTATS_SITEDOMAIN')
    ENV.delete('AWSTATS_PATH')
  end

  @rake_tasks << [[:pages,:store], :store_pages, 'store HTML pages to S3']
  def store_pages
    Dir["#{sitename}/html/*"].each do |f_name|
      puts "sending #{f_name}"
      AWS::S3::S3Object.store( f_name, open( f_name ), bucket, :access => :public_read )
    end
  end

  @rake_tasks << [:run, :run, 'run data cycle']
  def run
    fetch_data
    fetch_logs
    create_config
    run_stats
    store_data
    # store_logs
    create_pages
    store_pages
  end
end


if __FILE__ == $PROGRAM_NAME
  require 'test/unit'
  require 'rubygems'
  require 'mocha'

  class LiquiLogsWorkerCreationTest < Test::Unit::TestCase
    def test_for_creation_class_method
      l = LiquiLogs::Worker.create
      assert l.class, LiquiLogs::Worker
    end
    def test_creation_with_hash
      hsh = {}
      l = LiquiLogs::Worker.create hsh
      assert l.class, LiquiLogs::Worker
    end
  end

  class LiquiLogsWorkerRunTest < Test::Unit::TestCase
    def test_for_stats_run_method
      Object.expects(:system).returns('hello')
#      system('test')
      # l = LiquiLogs::Worker.create
      # l.stats_run
    end
  end
end
