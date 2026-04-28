# frozen_string_literal: true

# Parity test: GoldLapel::Instance (sync) vs GoldLapel::Async::Instance (async).
#
# Both classes are hand-written method-by-method — every public method on the
# sync surface has a matching method on the async surface (and vice versa)
# with an identical parameter signature. The async body delegates to
# `GoldLapel::Async::Utils` (native non-blocking pg variants) instead of the
# top-level `GoldLapel.*` module functions, but the wrapper signature shape is
# identical: same name, same positional args, same keyword args (including
# `conn:` overrides), same default values.
#
# This test exists to catch the silent-drift class of bug: someone adds a new
# method to one surface and forgets the other. Pure name-set comparison plus
# `Method#parameters` equality is enough to flag every case we've seen.
#
# When a method legitimately exists on only one side (e.g. an internal helper
# that has no async/sync analogue), add it to ASYNC_ONLY or SYNC_ONLY below
# with a comment explaining why. The test should fail loudly until the skip
# is documented — drift discipline beats convenience.

require "minitest/autorun"
require "goldlapel"
require "goldlapel/async"

class TestAsyncParity < Minitest::Test
  # Methods that legitimately exist on only one side. Empty today — both
  # surfaces are full mirrors. If a sync-only or async-only method is added
  # later, document the reason here.
  SYNC_ONLY = [].freeze
  ASYNC_ONLY = [].freeze

  def sync_methods
    (GoldLapel::Instance.instance_methods(false) - Object.instance_methods).sort
  end

  def async_methods
    (GoldLapel::Async::Instance.instance_methods(false) - Object.instance_methods).sort
  end

  def test_sync_methods_present_on_async
    missing = sync_methods - async_methods - SYNC_ONLY
    assert_empty missing,
      "Methods on GoldLapel::Instance but missing from GoldLapel::Async::Instance: " \
      "#{missing.inspect}. Either add the method to the async surface, or add it " \
      "to SYNC_ONLY in this test with a comment explaining why."
  end

  def test_async_methods_present_on_sync
    missing = async_methods - sync_methods - ASYNC_ONLY
    assert_empty missing,
      "Methods on GoldLapel::Async::Instance but missing from GoldLapel::Instance: " \
      "#{missing.inspect}. Either add the method to the sync surface, or add it " \
      "to ASYNC_ONLY in this test with a comment explaining why."
  end

  def test_method_signatures_match
    shared = sync_methods & async_methods
    drifted = []
    shared.each do |name|
      sync_params = GoldLapel::Instance.instance_method(name).parameters
      async_params = GoldLapel::Async::Instance.instance_method(name).parameters
      drifted << [name, sync_params, async_params] unless sync_params == async_params
    end
    if drifted.any?
      report = drifted.map { |n, s, a| "  :#{n}\n    sync:  #{s.inspect}\n    async: #{a.inspect}" }.join("\n")
      flunk "Method signatures drifted between sync and async surfaces:\n#{report}"
    end
  end
end
