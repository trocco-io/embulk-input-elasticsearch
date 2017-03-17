require_relative './helper'

Elasticsearch = Embulk::Input::Elasticsearch

module Embulk
  class Input::Elasticsearch
    class TestTransaction < Test::Unit::TestCase
      sub_test_case "get_slice_from_num_threads" do
        def test_normal
          slice = InputThread.get_slice_from_num_threads((1..10).to_a, 5)
          assert_equal slice.size, 5
          assert_equal slice.first.size, 2
        end

        def test_normal_same
          slice = InputThread.get_slice_from_num_threads((1..3).to_a, 3)
          assert_equal slice.size, 3
          assert_equal slice.first.size, 1
        end

        def test_num_threads_over_array_size
          slice = InputThread.get_slice_from_num_threads((1..3).to_a, 10)
          assert_equal slice.size, 3
          assert_equal slice.first.size, 1
        end

        def test_rest
          slice = InputThread.get_slice_from_num_threads((1..20).to_a, 8)
          assert_equal slice.size, 7
          assert_equal slice.first.size, 3
        end
      end
    end
  end
end

