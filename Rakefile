
require 'lib/liquilogs'

LiquiLogs::Worker.rake_tasks.each do |taskname, methodname,  description|
  desc description
  task taskname, :site do |t, args|
    puts "~~~ in task #{t.name} with #{args.inspect}"
    LiquiLogs::Worker.create(args[:site]).send methodname
  end
end

desc 'run all'
task :run_all do
  require 'yaml'
  config_file = YAML::load(File.read('sites.yml'))
#  config_file.keys.each do |site|
  %w{rubypulse cloudfront}.each do |site|
    puts "**** running for #{site} ****"
    t = Rake::Task['run']
    t.reenable
    t.invoke( site )
    puts "**** finished #{site} run ****"
  end
end

desc 'test'
task :test do
  puts "#{ENV}"
end
