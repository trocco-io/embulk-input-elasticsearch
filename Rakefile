require "bundler/gem_tasks"

task default: :build

desc "Run tests"
task :test do
  require "test-unit"

  Test::Unit::AutoRunner.run(true, './')
end
