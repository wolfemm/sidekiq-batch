# frozen_string_literal: true

module Sidekiq
  class Batch
    module Event
      SUCCESS = "success"
      COMPLETE = "complete"
      FAILED = "failed"

      class << self
        def success_or_complete?(event)
          case event.to_s
          when SUCCESS, COMPLETE
            true
          else
            false
          end
        end
      end
    end
  end
end