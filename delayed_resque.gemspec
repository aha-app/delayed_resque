$:.push File.expand_path("../lib", __FILE__)
require "delayed_resque/version"

Gem::Specification.new do |s|
  s.name = "delayed_resque"
  s.version = DelayedResque::VERSION
  s.platform = Gem::Platform::RUBY
  s.authors = ["k1w1"]
  s.email = ["k1w1@k1w1.org"]
  s.homepage = "http://github.com/k1w1/delayed_resque"
  s.summary = %q{todo}
  s.description = %q{todo}

  s.files = Dir["lib/**/*" "README.md", "MIT-LICENSE"]
  s.require_paths = ["lib"]
  s.test_files = Dir.glob('spec/**/*')

  s.add_dependency 'rails', '~> 6.1'
  s.add_dependency 'resque'
  s.add_dependency 'resque-scheduler'

  s.add_development_dependency "rspec"
end
