require "embulk/command/embulk_run"
require "embulk"
Embulk.setup

require "embulk/input/elasticsearch"
module Embulk
  module Input
    class ElasticsearchInputPluginTest < Test::Unit::TestCase
    end
  end
end
