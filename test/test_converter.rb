require_relative './helper'

Elasticsearch = Embulk::Input::Elasticsearch

module Embulk
  class Input::Elasticsearch
    class TestConverter < Test::Unit::TestCase

      def startup
      end

      def shutdown
      end

      sub_test_case "get_sources" do
        def test_normal
          fields = [
            {"name"=>"_id", "type"=>"string", "metadata"=>true},
            {"name"=>"product_id", "type"=>"long"},
            {"name"=>"title", "type"=>"string"}
          ]

          results = {
            "_scroll_id"=>"cXVlcnlUaGVuRmV0Y2g7NTsxNzg3MjE6WlphQ3V0WDNRYmFRcS1QQ3dCb2s5UTsxNzg3MjI6WlphQ3V0WDNRYmFRcS1QQ3dCb2s5UTsxNzg3MjM6WlphQ3V0WDNRYmFRcS1QQ3dCb2s5UTsxNzg3MjU6WlphQ3V0WDNRYmFRcS1QQ3dCb2s5UTsxNzg3MjQ6WlphQ3V0WDNRYmFRcS1QQ3dCb2s5UTswOw==",
            "took"=>41,
            "timed_out"=>false,
            "_shards"=>{"total"=>5, "successful"=>5, "failed"=>0},
            "hits"=>{
              "total"=>1,
              "max_score"=>nil,
              "hits"=>[
                {
                  "_index"=>"test_index",
                  "_type"=>"test_type",
                  "_id"=>"AVTCxiCuNR-BVKOgUB7R",
                  "_score"=>nil,
                  "_source"=>{
                    "title"=>"dummy title",
                    "product_id"=>1
                  },
                  "sort"=>[12534]
                }
              ]
            }
          }
          assert_equal Converter.get_sources(results, fields), [["AVTCxiCuNR-BVKOgUB7R", 1, "dummy title"]]
        end
      end
    end
  end
end
