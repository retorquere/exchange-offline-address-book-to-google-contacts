#!/usr/bin/env ruby

require 'base64'
require 'csv'
require 'ffi'
require 'json'
require 'net/http'
require 'nokogiri'
require 'oauth2'
require 'openssl'
require 'open-uri'
require 'ostruct'
require 'pp'
require 'shellwords'
require 'uri'
require 'yaml'
require_relative 'OabReader'

class Nokogiri::XML::Node
  attr_accessor :backup
end

def telephone(num)
  _num = "#{num}"
  return num unless _num =~ /[0-9]/
  _num.gsub!(/^\+/, '00')
  _num.gsub!(/[^0-9]/, '')
  _num.gsub!(/^0031/, '0')
  _num.gsub!(/^00/, '+')
  return _num
end

class GoogleContacts
  @@redirect = 'urn:ietf:wg:oauth:2.0:oob'
  @@ns = {
    'default' => "http://www.w3.org/2005/Atom",
    'batch' => "http://schemas.google.com/gdata/batch",
    'gContact' => "http://schemas.google.com/contact/2008",
    'gd' => "http://schemas.google.com/g/2005",
    'openSearch' => "http://a9.com/-/spec/opensearchrss/1.0/"
  }

  def initialize(code=nil)
    secret = JSON.parse(open('client_secret.json').read)['installed']
    @client = OAuth2::Client.new(secret['client_id'], secret['client_secret'], site: 'https://accounts.google.com', token_url: '/o/oauth2/token', authorize_url: '/o/oauth2/auth')
    login(code)
  end

  def login(code=nil)
    if code
      @token = @client.auth_code.get_token(code, :redirect_uri => @@redirect)
      open('.token.yml', 'w'){|f| f.write(@token.to_hash.to_yaml)}
    elsif File.file?('.token.yml')
      token = OAuth2::AccessToken.from_hash(@client, YAML::load_file('.token.yml'))
      @token = token.refresh!
    end

    if !@token || @token.expired?
      puts @client.auth_code.authorize_url(scope: 'https://www.google.com/m8/feeds', redirect_uri: @@redirect, access_type: :offline, approval_prompt: :force)
      exit
    end
  end

  def get(url)
    url = "https://www.google.com/m8/feeds#{url}" unless url =~ /^https?:/
    puts ":: #{url}"
    data = @token.get(url, {'GData-Version' => '3.0'})
    return Nokogiri::XML(data.body)
  end

  def han
    return @han if @han
    groups = get('/groups/default/full')
    @han = groups.at("//xmlns:entry[xmlns:title/text()='HAN']/xmlns:id/text()").to_xml
  end

  def contacts
    if !@contacts
      #@contacts = get("/contacts/default/full?max-results=10000")
      @contacts = get("/contacts/default/full?max-results=10")
      #puts contacts.to_xml
      @contacts.xpath('//xmlns:entry').each{|contact|
        email = contact.xpath('./gd:email').collect{|address| address['address'] }.compact.collect{|address| address.downcase }.detect{|address| address =~ /@han.nl$/ }
        if email
          contact.backup = contact.to_xml
        else
          contact.unlink
        end
      }
      puts "#{@contacts.xpath('//xmlns:entry').length} HAN contacts"
    end
    return @contacts
  end

  def merge(contact)
    return if contact.email.to_s == '' || contact.numbers.length == 0

    puts "merging #{contact.email}"
    gcontact = contacts.at("//xmlns:entry[gd:email[translate(@address,'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz')='#{contact.email.downcase}']]")
    if gcontact
      numbers = contact.numbers.dup
      gcontact.xpath('.//gd:phoneNumber').each{|n|
        number = telephone(n.content)
        if numbers.include?(number)
          n['label'] = (number =~ /^06/ ? 'Work mobile' : 'Work')
          numbers.delete(number)
        else
          n.unlink if n['label'] =~ /work/i
        end
      }
      numbers.each{|number|
        Nokogiri::XML::Builder.with(gcontact) do |xml|
          xml['gd'].phoneNumber('label' => (number =~ /^06/ ? 'Work mobile' : 'Work')) { number }
        end
      }
      if !gcontact.at('.//gd:name')
        Nokogiri::XML::Builder.with(gcontact) do |xml|
          xml['gd'].name {
            xml['gd'].givenName { contact.givenName }
            xml['gd'].familyName { contact.familyNameame }
            xml['gd'].fullName { contact.fullName }
          }
        end
      end
    else
      Nokogiri::XML::Builder.with(contacts.at('xmlns:feed')) do |xml|
        xml.entry('xmlns:gd' => "http://schemas.google.com/g/2005", 'xmlns:gContact' => "http://schemas.google.com/contact/2008") {
          xml.category('scheme'=>"http://schemas.google.com/g/2005#kind", 'term'=>"http://schemas.google.com/contact/2008#contact")
          xml.title('type' => 'text') { contact.fullName }
          xml['gd'].organization('rel' => "http://schemas.google.com/g/2005#work", 'primary' => "true") {
            xml['gd'].orgName { 'HAN' }
          }
          xml['gd'].email(rel: 'http://schemas.google.com/g/2005#work', address: contact.email)
          contact.numbers.each{|number|
            xml['gd'].phoneNumber(label: (number =~ /^06/ ? 'Work mobile' : 'Work')) { number }
          }
          xml['gContact'].groupMembershipInfo(deleted: "false", href: han)
          xml['gd'].name {
            xml['gd'].givenName { contact.givenName }
            xml['gd'].familyName { contact.familyNameame }
            xml['gd'].fullName { contact.fullName }
          }
        }
      end
    end
  end

  def save
    batch = 30
    contacts.xpath('//xmlns:entry').each{|contact|
      contact.unlink if contact.to_xml == contact.backup
      batch -= 1
      contact.unlink if batch < 0
    }
    puts contacts.to_xml
  end
end

module MSPack
  extend FFI::Library
  ffi_lib 'mspack'
  attach_function :mspack_create_oab_decompressor, [ :pointer ], :pointer
  attach_function :mspack_destroy_oab_decompressor, [ :pointer ], :void

  class MSOABDecompressor < FFI::Struct
    layout :decompress, callback([:pointer, :string, :string], :int),
           :decompress_incremental, callback([:pointer, :string, :string, :string], :int)
  end

end
def cast_to_msoab(pointer)
  return MSPack::MSOABDecompressor.new(pointer)
end

def decompress(source, target)
  c = MSPack.mspack_create_oab_decompressor(nil)
  msoab = cast_to_msoab(c)
  msoab[:decompress].call(c, source, target)
  MSPack.mspack_destroy_oab_decompressor(c)
end

class OAB
  def download(file)
    File.unlink(file) if File.file?(file)
    url = "#{@credentials.oab}/#{File.basename(file)}"
    system "wget --quiet --user=#{@credentials.user.shellescape} --password=#{@credentials.password.shellescape} -O #{file.shellescape} #{url.shellescape}"
  end

  def value(p, dflt=nil)
    return p ? (p[0] || dflt) : dflt
  end

  def initialize(online=true)
    @credentials = OpenStruct.new(YAML.load_file('.exchange.yml'))

    puts "loading GAL"
    @oab = OpenStruct.new({ pointer: File.expand_path(File.join(File.dirname(__FILE__), 'oab.xml')) })
    download(@oab.pointer)
    gal = Nokogiri::XML(open(@oab.pointer))
    @oab.compressed = File.expand_path(File.join(File.dirname(__FILE__), gal.at('//Full').inner_text))
    @oab.uncompressed = @oab.compressed.sub(/\.lzx$/, '') + '.oab'
    if !File.file?(@oab.uncompressed)
      download(@oab.compressed)
      decompress(@oab.compressed, @oab.uncompressed)
    end
    @oab.data = OabReader.new(@oab.uncompressed)
  end

  def each
    return enum_for(:each) unless block_given? # Sparkling magic!

    @oab.data.records.each{|record|
      r = OpenStruct.new({
        email: value(record.SmtpAddress).to_s.strip,
        numbers: [:BusinessTelephoneNumber, :Business2TelephoneNumber, :Assistant, :AssistantTelephoneNumber, :MobileTelephoneNumber].collect{|k| record[k] || []}.flatten,
        givenName: value(record.GivenName),
        familyName: value(record.Surname),
        fullName: value(record.DisplayName, value(record.SmtpAddress).sub(/@.*/, '').gsub('.', ' '))
      })
      (record.PostalCode || []).each{|pc| # wow
        next unless pc =~/^(0|\+)[-0-9\s]+$/
        r.numbers << pc
      }
      r.numbers = r.numbers.collect{|n| telephone(n)}.reject{|n| n.to_s.strip == ''}.uniq.sort
      yield r
    }
  end
end

gc = GoogleContacts.new(ARGV[0])
oab = OAB.new
oab.each{|record| gc.merge(record) }
gc.save
