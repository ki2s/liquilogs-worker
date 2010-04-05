#
#
#

require 'pathname'
require 'fileutils'
require 'rubygems'

module LiquiLogs; end

class LiquiLogs::Worker
  def initialize config=nil
    require 'ostruct'
    @config= OpenStruct.new(
                            :sitename   => 'rubypulse',
                            :bucket     => 'rubypulse-logs',
                            :log_prefix => 'log-s3/access_log',
                            :conf_type  => 's3',
                            # alex@ki2s.com
                            :aws_key    => '"AKIAIRHLSVBIOYHXVQTQ"',
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
  def bucket; @config.bucket; end
  def log_prefix; @config.log_prefix; end
  def conf_type; @config.conf_type; end
  def ll; "liquilogs"; end

  def fetch_data
    # only fetch data if dir is empty
    if Dir["#{sitename}/data/*"].empty?
      FileUtils.mkpath "#{sitename}/data" unless File.exists? "#{sitename}/data"
      o = AWS::S3::S3Object.find "#{ll}/data.#{sitename}.tgz", bucket
      IO.popen("tar zxf - -C #{sitename}/data", 'w') { |p| p.print o.value }
    end
  end

  def store_data
    file_list = Pathname.glob( sitedir+'data'+ '*').map( &:basename).join(' ')
    IO.popen( "tar c -C #{ sitedir+'data' } #{file_list} | gzip -9fc" ) do |io|
#      store_url = "#{ll}/data.#{sitename}.#{Time.now.strftime('%Y%m%d-%H%M%S')}.tgz"
#      store_url = "#{ll}/data.#{sitename}.tgz"
      store_url = "#{ll}/data.#{sitename}.temp.tgz"
      puts "storing #{store_url}"
      AWS::S3::S3Object.store( store_url, io.read, bucket, :access => :public_read )
    end
  end

  def fetch_logs
    (sitedir + 'logs').open('w') do |file|
      AWS::S3::Bucket.objects( bucket, :prefix => log_prefix).each do |s3o|
        s3o.value { |chunk| file.write(chunk) }
      end
    end
  end

  def create_config
    unless File.exists? Pathname.pwd + 'awstats' + 'wwwroot' + 'cgi-bin' + "awstats.#{sitename}.conf"
      cd Pathname.pwd + 'awstats' + 'wwwroot' + 'cgi-bin', :verbose => false do
        FileUtils.ln_sf "awstats.conf.#{conf_type}.template", "awstats.#{sitename}.conf"
      end
    end
  end

  def run_stats
    ENV['AWSTATS_PATH']= Pathname.pwd
    ENV['AWSTATS_SITEDOMAIN']= sitename

    system( "awstats/wwwroot/cgi-bin/awstats.pl -config=#{sitename} -update" )

    ENV.delete('AWSTATS_SITEDOMAIN')
    ENV.delete('AWSTATS_PATH')
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
