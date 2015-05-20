#!/usr/bin/env ruby

require 'oauth2'
require 'pp'
require 'yaml'
require 'json'
require 'nokogiri'
require 'openssl'

class GoogleContacts
  @@redirect = 'urn:ietf:wg:oauth:2.0:oob'

  def initialize
    secret = JSON.parse(open('client_secret.json').read)['installed']
    certs = nil
    `openssl version -a`.split(/\n/).detect{|line| line.strip =~ /^OPENSSLDIR: "(.*)"$/ && certs = $1 }
    raise "No certificate directory found" unless certs
    certs = File.join(certs, 'certs')
    raise "#{certs} is not a directory" unless File.directory?(certs)
    puts certs
    @client = OAuth2::Client.new(secret['client_id'], secret['client_secret'], site: 'https://accounts.google.com', token_url: '/o/oauth2/token', authorize_url: '/o/oauth2/auth')
  end
  attr_reader :token

  def login(code=nil)
    if code
      @token = @client.auth_code.get_token(code, :redirect_uri => @@redirect)
      open('.token.yml', 'w'){|f| f.write(@token.to_hash.to_yaml)}
    elsif File.file?('.token.yml')
      token = OAuth2::AccessToken.from_hash(@client, YAML::load_file('.token.yml'))
      @token = token.refresh!
    end

    if @token.expired
      puts @client.auth_code.authorize_url(scope: 'https://www.google.com/m8/feeds', redirect_uri: @@redirect, access_type: :offline, approval_prompt: :force)
      exit
    end
  end
end

gc = GoogleContacts.new
gc.login(ARGV[0])
userEmail = 'emiliano.heyns@iris-advies.com'
userEmail = 'default'
groups = Nokogiri::XML(gc.token.get("https://www.google.com/m8/feeds/groups/default/full", {'GData-Version' => '3.0'}).body)
groupID = groups.at("//xmlns:entry[xmlns:title/text()='HAN']/xmlns:id/text()").to_xml
puts groupID
contacts = Nokogiri::XML(gc.token.get("https://www.google.com/m8/feeds/contacts/default/full?max_result=10000&group=#{groupID}", {'GData-Version' => '3.0'}).body)
puts contacts.to_xml
