#!/usr/bin/env ruby

require 'test/unit'

# require 'embulk/java/bootstrap'
require 'embulk'
begin
  # Embulk ~> 0.8.x
  Embulk.setup
rescue NotImplementedError
  # Embulk ~> 0.9.x
  require 'embulk/java/bootstrap'
end
Embulk.logger = Embulk::Logger.new('/dev/null')

APP_ROOT = File.expand_path('../', __dir__)
TEST_ROOT = File.expand_path(File.dirname(__FILE__))

require 'embulk/input/elasticsearch'
