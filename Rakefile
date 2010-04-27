
require 'lib/liquilogs'

LiquiLogs::Worker.rake_tasks.each do |taskname, methodname,  description|
  desc description
  task taskname do
    LiquiLogs::Worker.create.send methodname
  end
end

