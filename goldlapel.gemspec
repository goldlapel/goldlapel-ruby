# frozen_string_literal: true

Gem::Specification.new do |spec|
  spec.name = "goldlapel"
  spec.version = ENV.fetch("GEM_VERSION", "0.1.0")
  spec.platform = ENV["GEM_PLATFORM"] if ENV["GEM_PLATFORM"]
  spec.authors = ["Stephen Gibson"]
  spec.summary = "Self-optimizing Postgres proxy — automatic materialized views and indexes"
  spec.description = "Gold Lapel sits between your app and Postgres, watches query patterns, " \
                     "and automatically creates materialized views and indexes to make your " \
                     "database faster. Zero code changes required."
  spec.homepage = "https://goldlapel.com"
  spec.license = "Proprietary"

  spec.required_ruby_version = ">= 3.2.0"

  spec.metadata["homepage_uri"] = spec.homepage
  spec.metadata["source_code_uri"] = "https://github.com/goldlapel/goldlapel-ruby"
  spec.bindir = "exe"
  spec.executables = ["goldlapel"]
  spec.files = Dir["lib/**/*.rb", "bin/*", "exe/*", "README.md"]
  spec.require_paths = ["lib"]
end
