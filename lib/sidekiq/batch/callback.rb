# frozen_string_literal: true

module Sidekiq
  class Batch
    module Callback
      class Worker
        include Sidekiq::Worker

        def perform(clazz, event, opts, bid, parent_bid)
          return unless Sidekiq::Batch::Event.success_or_complete?(event)
          clazz, method = clazz.split("#") if (clazz && clazz.class == String && clazz.include?("#"))
          method = "on_#{event}" if method.nil?
          status = Sidekiq::Batch::Status.new(bid)

          if clazz && object = Object.const_get(clazz)
            instance = object.new
            instance.send(method, status, opts) if instance.respond_to?(method)
          end
        end
      end

      class Finalize
        def dispatch status, opts
          bid = opts["bid"]
          callback_bid = status.bid
          event = opts["event"]
          callback_batch = bid != callback_bid

          Sidekiq.logger.debug {"Finalize #{event} batch id: #{opts["bid"]}, callback batch id: #{callback_bid} callback_batch #{callback_batch}"}

          batch_status = Status.new bid
          send(event, bid, batch_status, batch_status.parent_bid)


          # Different events are run in different callback batches
          bids_to_clean = []

          if callback_batch
            bids_to_clean << callback_bid
          end

          if event == Sidekiq::Batch::Event::SUCCESS
            bids_to_clean << bid
          end

          unless bids_to_clean.empty?
            Sidekiq::Batch.cleanup_redis(*bids_to_clean)
          end
        end

        def success(bid, status, parent_bid)
          return unless parent_bid

          _, _, success, _, complete, pending, children, failure = Sidekiq.redis do |r|
            r.multi do |pipeline|
              parent_batch_key = "BID-#{parent_bid}"

              pipeline.sadd("#{parent_batch_key}-#{Sidekiq::Batch::Event::SUCCESS}", [bid])
              pipeline.expire("#{parent_batch_key}-#{Sidekiq::Batch::Event::SUCCESS}", Sidekiq::Batch::BID_EXPIRE_TTL)
              pipeline.scard("#{parent_batch_key}-#{Sidekiq::Batch::Event::SUCCESS}")
              pipeline.sadd("#{parent_batch_key}-#{Sidekiq::Batch::Event::COMPLETE}", [bid])
              pipeline.scard("#{parent_batch_key}-#{Sidekiq::Batch::Event::COMPLETE}")
              pipeline.hincrby(parent_batch_key, "pending", 0)
              pipeline.hincrby(parent_batch_key, "children", 0)
              pipeline.scard("#{parent_batch_key}-#{Sidekiq::Batch::Event::FAILED}")
            end
          end
          # if job finished successfully and parent batch completed call parent complete callback
          # Success callback is called after complete callback
          if complete == children && pending == failure
            Sidekiq.logger.debug {"Finalize parent complete bid: #{parent_bid}"}
            Batch.enqueue_callbacks(Sidekiq::Batch::Event::COMPLETE, parent_bid)
          end

        end

        def complete(bid, status, parent_bid)
          pending, children, success = Sidekiq.redis do |r|
            batch_key = "BID-#{bid}"

            r.multi do |pipeline|
              pipeline.hincrby(batch_key, "pending", 0)
              pipeline.hincrby(batch_key, "children", 0)
              pipeline.scard("#{batch_key}-#{Sidekiq::Batch::Event::SUCCESS}")
            end
          end

          # if we batch was successful run success callback
          if pending.to_i.zero? && children == success
            Batch.enqueue_callbacks(Sidekiq::Batch::Event::SUCCESS, bid)

          elsif parent_bid
            # if batch was not successfull check and see if its parent is complete
            # if the parent is complete we trigger the complete callback
            # We don't want to run this if the batch was successfull because the success
            # callback may add more jobs to the parent batch

            Sidekiq.logger.debug {"Finalize parent complete bid: #{parent_bid}"}
            _, complete, pending, children, failure = Sidekiq.redis do |r|
              parent_batch_key = "BID-#{parent_bid}"

              r.multi do |pipeline|
                pipeline.sadd("#{parent_batch_key}-#{Sidekiq::Batch::Event::COMPLETE}", [bid])
                pipeline.scard("#{parent_batch_key}-#{Sidekiq::Batch::Event::COMPLETE}")
                pipeline.hincrby(parent_batch_key, "pending", 0)
                pipeline.hincrby(parent_batch_key, "children", 0)
                pipeline.scard("#{parent_batch_key}-#{Sidekiq::Batch::Event::FAILED}")
              end
            end
            if complete == children && pending == failure
              Batch.enqueue_callbacks(Sidekiq::Batch::Event::COMPLETE, parent_bid)
            end
          end
        end

        def cleanup_redis(bid, callback_bid=nil)
          bids_to_clean = [bid]

          if callback_bid
            bids_to_clean << callback_bid
          end

          Sidekiq::Batch.cleanup_redis(*bids_to_clean)
        end
      end
    end
  end
end
