# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "goldlapel"
  spec.version = ENV.fetch("GEM_VERSION", "0.0.0")
  spec.platform = ENV["GEM_PLATFORM"] if ENV["GEM_PLATFORM"]
  spec.authors = ["The Waiter of Gold Lapel"]
  spec.email = ["thewaiter@goldlapel.com"]
  spec.summary = "Self-optimizing Postgres proxy — shared result cache and automatic indexes"
  spec.description = "Gold Lapel sits between your app and Postgres, serves repeated reads from " \
                     "a result cache shared by every connection, and creates the indexes your " \
                     "query patterns need. The gem runs the proxy for you and adds " \
                     "Postgres-backed helpers: search, documents, streams, queues, counters " \
                     "and more. Zero code changes required."
  spec.homepage = "https://goldlapel.com"
  spec.license = "MIT"

  spec.required_ruby_version = ">= 3.2.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/goldlapel/goldlapel-ruby"
  spec.metadata["bug_tracker_uri"] = "https://github.com/goldlapel/goldlapel-ruby/issues"
  spec.metadata["documentation_uri"] = "https://goldlapel.com/docs/ruby"
  spec.bindir = "exe"
  spec.executables = ["goldlapel"]
  spec.files = Dir["lib/**/*.rb", "bin/*", "exe/*", "README.md", "LICENSE", "THIRD-PARTY-NOTICES.md"]
  spec.require_paths = ["lib"]
end
