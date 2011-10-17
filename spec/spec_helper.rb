require 'rubygems'
require 'bundler'
Bundler.setup :default
Bundler.require :default

dir = File.dirname(File.expand_path(__FILE__))
$LOAD_PATH.unshift dir + '/../lib'
