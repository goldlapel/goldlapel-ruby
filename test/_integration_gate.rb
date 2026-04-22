# frozen_string_literal: true

# Shared integration-test gating — standardized across all Gold Lapel wrappers.
#
# Convention:
#   - GOLDLAPEL_INTEGRATION=1  — explicit opt-in gate
#   - GOLDLAPEL_TEST_UPSTREAM  — Postgres URL for the test upstream
#
# Both must be set. If GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM
# is missing, the gate raises loudly — this prevents a half-configured CI from
# silently skipping integration tests and producing a false-green unit-only run.
#
# If GOLDLAPEL_INTEGRATION is unset, integration tests skip silently.

module GoldLapelTestGate
  # Returns the upstream Postgres URL if integration tests should run, or nil
  # if they should skip. Raises RuntimeError loudly if GOLDLAPEL_INTEGRATION=1
  # is set but GOLDLAPEL_TEST_UPSTREAM is missing (false-green prevention).
  #
  # Skip callers should pair this with their test framework's skip mechanism,
  # e.g. Minitest#skip(GoldLapelTestGate.skip_reason) when this returns nil.
  def self.integration_upstream
    integration = ENV["GOLDLAPEL_INTEGRATION"] == "1"
    upstream = ENV["GOLDLAPEL_TEST_UPSTREAM"]

    if integration && (upstream.nil? || upstream.empty?)
      raise "GOLDLAPEL_INTEGRATION=1 is set but GOLDLAPEL_TEST_UPSTREAM is " \
            "missing. Set GOLDLAPEL_TEST_UPSTREAM to a Postgres URL " \
            "(e.g. postgresql://postgres@localhost/postgres) or unset " \
            "GOLDLAPEL_INTEGRATION to skip integration tests."
    end

    integration ? upstream : nil
  end

  def self.should_run?
    !integration_upstream.nil?
  end

  def self.skip_reason
    "set GOLDLAPEL_INTEGRATION=1 and GOLDLAPEL_TEST_UPSTREAM to run"
  end
end
