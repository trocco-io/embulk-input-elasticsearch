module Embulk
  module Input
    class Elasticsearch < InputPlugin
      class InputThread
        def self.get_slice_from_num_threads(array, define_num_threads)
          num_threads = array.size < define_num_threads ? array.size : define_num_threads
          per_queries = if (array.size % num_threads) == 0
            (array.size / num_threads)
          else
            (array.size / num_threads) + 1
          end
          sliced = array.each_slice(per_queries).to_a
          Embulk.logger.info("calculate num threads => #{sliced.size}")
          return sliced
        end
      end
    end
  end
end
