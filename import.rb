#!/usr/bin/env ruby

require 'base64'
require 'csv'
require 'ffi'
require 'json'
require 'namae'
require 'net/http'
require 'nokogiri'
require 'oauth2'
require 'openssl'
require 'open-uri'
require 'ostruct'
require 'phony'
require 'pp'
require 'shellwords'
require 'uri'
require 'yaml'
require_relative 'OabReader'

class Nokogiri::XML::Node
  attr_accessor :contact_backup
  attr_accessor :contact_insert
end

def telephone(num)
  _num = "#{num}".strip
  _num.gsub!(/^\+/, '00')
  _num.gsub!(/^0031/, '0')
  _num.gsub!(/^00/, '+')
  return nil unless Phony.plausible?(_num)
  return Phony.format(Phony.normalize(_num), format: :international)
end

def label(num, prefix)
  prefix = [prefix]
  parts = Phony.split(Phony.normalize(telephone(num)))
  prefix << 'mobile' if parts[0,2] == ['31', '6']
  return prefix.join(' ')
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
    if code == 'offline'
      @offline = true
      return
    end

    if code
      @token = @client.auth_code.get_token(code, :redirect_uri => @@redirect)
      open('.token.yml', 'w'){|f| f.write(@token.to_hash.to_yaml)}
    elsif File.file?('.token.yml')
      token = OAuth2::AccessToken.from_hash(@client, YAML::load_file('.token.yml'))
      @token = token.refresh!
    end

    if !@token || @token.expired?
      STDERR.puts @client.auth_code.authorize_url(scope: 'https://www.google.com/m8/feeds', redirect_uri: @@redirect, access_type: :offline, approval_prompt: :force)
      exit
    end
  end

  def get(url)
    throw "offline: #{url}" if @offline

    url = "https://www.google.com/m8/feeds#{url}" unless url =~ /^https?:/
    STDERR.puts ":: #{url}"
    data = @token.get(url, headers: {'GData-Version' => '3.0'})
    return Nokogiri::XML(data.body)
  end

  def han
    return 'offline' if @offline

    return @han if @han
    groups = get('/groups/default/full')
    @han = groups.at("//xmlns:entry[xmlns:title/text()='HAN']/xmlns:id/text()").to_xml
  end

  def han_email(contact)
    return contact.xpath('./gd:email').collect{|address| address['address'] }.compact.collect{|address| address.downcase }.detect{|address| address =~ /@han.nl$/ }
  end

  def contacts
    if !@contacts
      if @offline
        @contacts = Nokogiri::XML(open('contacts.xml'))
      else
        @contacts = get("/contacts/default/full?max-results=10000")
        open('contacts.xml', 'w'){|f| f.write(@contacts.to_xml) }
      end
      @contact = {}
      @contacts.xpath('//xmlns:entry').each{|contact|
        email = han_email(contact)
        if email
          @contact[email.downcase] = contact
        else
          contact.unlink
        end
      }
    end
    return @contacts
  end

  def merge(contact)
    return if contact.email.to_s == ''

    #puts "merging #{contact.email}"
    contacts
    gcontact = @contact[contact.email.downcase]
    if gcontact
      contact.contact_backup = contact.to_xml unless contact.contact_backup
      numbers = contact.numbers.dup
      gcontact.xpath('.//gd:phoneNumber').each{|n|
        number = telephone(n.content)
        if numbers.include?(number)
          # make sure work numbers are labeled as such
          n['label'] = label(number, 'Work')
          numbers.delete(number)
        else # remove work-labeled numbers that are not in the GAL
          n.unlink if n['label'] =~ /work/i
        end
      }

      # add remaining GAL numbers
      numbers.each{|number|
        Nokogiri::XML::Builder.with(gcontact) do |xml|
          xml['gd'].phoneNumber('label' => label(number, 'Work')) { xml.text(number) }
        end
      }

      # add to group HAN
      if !gcontact.at(".//gContact:groupMembershipInfo[@href='#{han}']")
        Nokogiri::XML::Builder.with(gcontact) do |xml|
          xml['gContact'].groupMembershipInfo(deleted: "false", href: han)
        end
      end

      # set structured name
      if !gcontact.at('.//gd:name')
        Nokogiri::XML::Builder.with(gcontact) do |xml|
          xml['gd'].name {
            xml['gd'].givenName { xml.text(contact.givenName) }
            xml['gd'].familyName { xml.text(contact.familyName) }
            xml['gd'].fullName { xml.text(contact.fullName) }
          }
        end
      end

    elsif contact.numbers.length != 0 # new contact with phone numbers
      Nokogiri::XML::Builder.with(contacts.at('//xmlns:feed')) do |xml|
        xml.entry('xmlns:gd' => "http://schemas.google.com/g/2005", 'xmlns:gContact' => "http://schemas.google.com/contact/2008") {
          xml.category('scheme'=>"http://schemas.google.com/g/2005#kind", 'term'=>"http://schemas.google.com/contact/2008#contact")
          xml.title('type' => 'text') { xml.text(contact.fullName) }
          xml['gd'].organization('rel' => "http://schemas.google.com/g/2005#work", 'primary' => "true") {
            xml['gd'].orgName { xml.text('HAN') }
          }
          xml['gd'].email(rel: 'http://schemas.google.com/g/2005#work', address: contact.email)
          contact.numbers.each{|number|
            xml['gd'].phoneNumber(label: (number =~ /^06/ ? 'Work mobile' : 'Work')) { xml.text(number) }
          }
          xml['gContact'].groupMembershipInfo(deleted: "false", href: han)
          xml['gd'].name {
            xml['gd'].givenName { xml.text(contact.givenName) }
            xml['gd'].familyName { xml.text(contact.familyName) }
            xml['gd'].fullName { xml.text(contact.fullName) }
          }
        }
      end
      gcontact = @contact[contact.email.downcase] = contacts.at("//xmlns:entry[gd:email[@address='#{contact.email}']]")
      gcontact.contact_insert = true
    end
  end

  def save
    status = OpenStruct.new(updated: 0, deleted: 0, inserted: 0, retained: 0)

    contacts.xpath('//xmlns:entry').each{|contact|
      id = han_email(contact)
      if contact.xpath('.//gd:phoneNumber').length == 0 || !(contact.contact_insert || contact.contact_backup)
        STDERR.puts "deleting #{id}: #{contact.xpath('.//gd:phoneNumber').length}, insert=#{!!contact.contact_insert}, exists=#{!!contact.contact_backup}"
        status.deleted += 1
        Nokogiri::XML::Builder.with(contact) do |xml|
          xml['batch'].id { xml.text("delete-#{id}") }
          xml['batch'].operation(type: 'delete')
        end
      elsif contact.contact_insert
        status.inserted += 1
        Nokogiri::XML::Builder.with(contact) do |xml|
          xml['batch'].id { xml.text("insert-#{id}") }
          xml['batch'].operation(type: 'insert')
        end
      elsif contact.to_xml != contact.contact_backup
        status.updated += 1
        Nokogiri::XML::Builder.with(contact) do |xml|
          xml['batch'].id { xml.text("update-#{id}") }
          xml['batch'].operation(type: 'update')
        end
      else
        status.retained += 1
        contact.unlink
      end
    }
    STDERR.puts status.inspect
    open('update.xml', 'w'){|f| f.write(contacts.to_xml) }
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
  def initialize(offline=false)
    @offline = offline
  end

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

    STDERR.puts "loading GAL"
    @oab = OpenStruct.new({ pointer: File.expand_path(File.join(File.dirname(__FILE__), 'oab.xml')) })
    download(@oab.pointer) unless @offline
    gal = Nokogiri::XML(open(@oab.pointer))
    @oab.compressed = File.expand_path(File.join(File.dirname(__FILE__), gal.at('//Full').inner_text))
    @oab.uncompressed = @oab.compressed.sub(/\.lzx$/, '') + '.oab'
    if !File.file?(@oab.uncompressed)
      throw 'cannot download in offline mode' if @offline
      Dir[File.expand_path(File.join(File.dirname(__FILE__), '*.lzx'))].each{|lzx| File.unlink(lzx) }
      Dir[File.expand_path(File.join(File.dirname(__FILE__), '*.oab'))].each{|oab| File.unlink(oab) }
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

      name = r.fullName.gsub(/#{r.familyName} /i, "#{r.familyName}, ")
      name = r.fullName.gsub(/(#{r.familyName}[^\s]+)/i) { "#{$1}," } if name == r.fullName
      if name != r.fullName
        name = name.split(',', 2).collect{|n| n.strip }.reverse.join(' ')
        r.fullName = name
      end

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
numbers = {}
oab.each{|record|
  numbers[record.email.downcase] ||= []
  numbers[record.email.downcase].concat(record.numbers)
  record.numbers = numbers[record.email.downcase].uniq.sort
  gc.merge(record)
}
gc.save
