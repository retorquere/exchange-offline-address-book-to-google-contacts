#!/usr/bin/env ruby

require 'csv'
require 'ffi'
require 'json'
require 'logging'
require 'net/http'
require 'nokogiri'
require 'oauth2'
require 'openssl'
require 'open-uri'
require 'ostruct'
require 'phony'
require 'pp'
require 'shellwords'
require 'singleton'
require 'uri'
require 'yaml'
require_relative 'OabReader'

require 'trollop'
OPTS = OpenStruct.new(Trollop::options {
  opt :action,  "delete/update/insert", :default => ''
  opt :batch,   "Batch updates"
  opt :code,    "Authorization code", :type => :string
  opt :domain,  "Work domain", :type => :string, :default => 'HAN.nl'
  opt :dry_run, "Dry-run"
  opt :force,   "Force update"
  opt :group,   "Contacts group", :type => :string, :default => 'Coworkers'
  opt :log,     "Log level", :type => :string, :default => 'warn'
  opt :names,   "Force name update"
  opt :offline, "Offline"
  opt :post,    "Post XML instruction", type: :string, default: ''
})

LOGGER = Logging.logger(STDOUT)
LOGGER.level = OPTS.log.intern

def telephone(num)
  _num = "#{num}".strip
  _num = "00#{_num}" if _num =~ /^31/
  _num.gsub!(/^\+/, '00')
  _num.gsub!(/^0([1-9])/) { "0031#{$1}" }
  _num.gsub!(/^00/, '+')
  return Phony.format(Phony.normalize(_num), format: :international) if Phony.plausible?(_num)
  LOGGER.debug "Not a plausible number: #{num} (#{_num})"
  return nil
end

def label(num, prefix)
  prefix = [prefix]
  parts = Phony.split(Phony.normalize(telephone(num)))
  prefix << 'Mobile' if parts[0,2] == ['31', '6']
  return prefix.join(' ')
end

class GoogleContacts
  include Singleton
  class Contact
    @@action = {}
    def initialize(contact)
      @node = contact

      # reset to 'keep' by merge
      delete! if id && !@@action[id]

      # cleanup from asynk
      @node.xpath('./gContact:userDefinedField').each{|udf| udf.unlink }
      @corp_email = @node.xpath('.//gd:email').collect{|address| address['address'] }.compact.collect{|address| address.downcase }.detect{|address| address =~ /@#{OPTS.domain}$/i }
    end
    attr_reader :node
    attr_reader :corp_email

    def id
      id = @node.at('./xmlns:id')
      return nil unless id
      return id.inner_text
    end

    def etag
      return @node['gd:etag']
    end

    def action
      return :insert unless id
      throw "#{id} has no action assigned?!" unless @@action[id]
      return @@action[id]
    end

    [:insert, :keep, :update, :delete].each{|status|
      define_method("#{status}!") do
        set_status(status)
      end
      define_method("#{status}?") do
        return get_status(status)
      end
    }

    def photo
      photo = @node.at("./xmlns:link[@rel='http://schemas.google.com/contacts/2008/rel#photo']")
      return false unless photo
      return OpenStruct.new(url: photo['href'], etag: photo['gd:etag'])
    end

    private

    def set_status(status)
      case status
        when :insert
          throw "contact with ID cannot be inserted" if id
        
        when :delete, :keep, :update
          throw "#{status}: contact without ID can only be inserted" if !id
          @@action[id] = status

        else
          throw "Unexpected status #{status.inspect}"
      end
    end
    def get_status
      return :insert unless id
      return @@action[id]
    end
  end

  @@redirect = 'urn:ietf:wg:oauth:2.0:oob'
  @@ns = {
    'default' => "http://www.w3.org/2005/Atom",
    'batch' => "http://schemas.google.com/gdata/batch",
    'gContact' => "http://schemas.google.com/contact/2008",
    'gd' => "http://schemas.google.com/g/2005",
    'openSearch' => "http://a9.com/-/spec/opensearchrss/1.0/"
  }

  def initialize
    secret = JSON.parse(open('client_secret.json').read)['installed']
    @client = OAuth2::Client.new(secret['client_id'], secret['client_secret'], site: 'https://accounts.google.com', token_url: '/o/oauth2/token', authorize_url: '/o/oauth2/auth')
    login

    if OPTS.offline
      groups = Nokogiri::XML(open('groups.xml'))
    else
      groups = get('/groups/default/full')
      open('groups.xml', 'w') {|f| f.write(groups.to_xml) }
    end
    @group = groups.at("//xmlns:entry[gContact:systemGroup/@id='#{OPTS.group}']/xmlns:id/text()")
    @group ||= groups.at("//xmlns:entry[xmlns:title/text()='#{OPTS.group}']/xmlns:id/text()")
    @group = @group.to_xml

    if OPTS.offline
      @contacts = Nokogiri::XML(open('contacts.xml'))
    else
      @contacts = get("/contacts/default/full?max-results=10000")
      open('contacts.xml', 'w'){|f| f.write(@contacts.to_xml) }
    end

    # remove non-corp contacts, register contacts, and remove duplicates
    @contact = {}
    @contacts.xpath('//xmlns:entry').each{|contact|
      contact = Contact.new(contact)

      if !contact.corp_email # not interesting
        contact.node.unlink
        next
      end

      if !@contact[contact.corp_email]
        @contact[contact.corp_email] = contact.node
      else # duplicate
        contact.delete!

        # put missing numbers on first hit
        original = Contact.new(@contact[contact.corp_email])
        numbers = original.node.xpath('.//gd:phoneNumber').collect{|n| telephone(n.inner_text) }
        contact.node.xpath('.//gd:phoneNumber').each{|n|
          coninue if numbers.include?(telephone(n.inner_text))
          n.parent = original.node
          original.update!
        }
      end
    }
  end

  def login
    return if OPTS.offline

    if OPTS.code
      @token = @client.auth_code.get_token(OPTS.code, :redirect_uri => @@redirect)
      open('.token.yml', 'w'){|f| f.write(@token.to_hash.to_yaml)}
    elsif File.file?('.token.yml')
      token = OAuth2::AccessToken.from_hash(@client, YAML::load_file('.token.yml'))
      @token = token.refresh!
    end

    if !@token || @token.expired?
      LOGGER.debug @client.auth_code.authorize_url(scope: 'https://www.google.com/m8/feeds', redirect_uri: @@redirect, access_type: :offline, approval_prompt: :force)
      exit
    end
  end

  def put(url, body, etag, content_type='application/atom+xml')
    throw "offline: #{url}" if OPTS.offline

    url = "https://www.google.com/m8/feeds#{url}" unless url =~ /^https?:/
    LOGGER.debug "PUT #{url}"
    headers = {'Content-Type' => content_type, 'GData-Version' => '3.0'}
    headers['If-Match'] = etag if etag
    data = @token.put(url, body: body, headers: headers)
    return Nokogiri::XML(data.body)
  end

  def delete(url, etag)
    throw "offline: #{url}" if OPTS.offline

    url = "https://www.google.com/m8/feeds#{url}" unless url =~ /^https?:/
    LOGGER.debug "DELETE #{url}"
    data = @token.delete(url, headers: {'If-Match' => etag, 'GData-Version' => '3.0'})
    return Nokogiri::XML(data.body)
  end

  def post(url, body)
    throw "offline: #{url}" if OPTS.offline

    url = "https://www.google.com/m8/feeds#{url}" unless url =~ /^https?:/
    LOGGER.debug "POST #{url}"
    data = @token.post(url, body: body, headers: {'Content-Type' => 'application/atom+xml', 'GData-Version' => '3.0'})
    return Nokogiri::XML(data.body)
  end

  def get(url)
    throw "offline: #{url}" if OPTS.offline

    url = "https://www.google.com/m8/feeds#{url}" unless url =~ /^https?:/
    LOGGER.debug "GET #{url}"
    data = @token.get(url, headers: {'GData-Version' => '3.0'})
    return Nokogiri::XML(data.body)
  end

  def merge(gal)
    return if gal.email.to_s == ''

    contact = @contact[gal.email.downcase]

    if !contact
      return if gal.numbers.length == 0 # new contact but no phone numbers

      LOGGER.debug "merge: new contact #{gal.email}"

      Nokogiri::XML::Builder.with(@contacts.at('//xmlns:feed')) do |xml|
        xml.entry('xmlns:gd' => "http://schemas.google.com/g/2005", 'xmlns:gContact' => "http://schemas.google.com/contact/2008") {
          xml.category('scheme'=>"http://schemas.google.com/g/2005#kind", 'term'=>"http://schemas.google.com/contact/2008#contact")
          xml.title('type' => 'text') { xml.text(gal.fullName) }
          xml['gd'].organization('rel' => "http://schemas.google.com/g/2005#work", 'primary' => "true") {
            xml['gd'].orgName { xml.text(OPTS.domain.gsub(/\..*/, '')) }
          }
          xml['gd'].email(rel: 'http://schemas.google.com/g/2005#work', address: gal.email)
          gal.numbers.each{|number|
            xml['gd'].phoneNumber(label: label(number, 'Work')) { xml.text(number) }
          }
          xml['gContact'].groupMembershipInfo(deleted: "false", href: @group)
          xml['gd'].name {
            xml['gd'].givenName { xml.text(gal.givenName) }
            xml['gd'].familyName { xml.text(gal.familyName) }
            xml['gd'].fullName { xml.text(gal.fullName) }
          }
        }
      end
      return
    end

    contact = Contact.new(contact)
    contact.keep!
    LOGGER.debug "merge: merging #{contact.corp_email}"

    numbers = gal.numbers.dup
    contact.node.xpath('.//gd:phoneNumber').each{|n|
      number = telephone(n.inner_text)
      if numbers.include?(number)
        # make sure work numbers are labeled as such
        l = label(number, 'Work')
        if n['label'] != l
          LOGGER.debug "#{gal.email}: work number #{number}"
          n['label'] = l
          n.delete('rel')
          contact.update!
        end
        numbers.delete(number)
      else # remove work-labeled numbers that are not in the GAL
        if n['label'] =~ /work/i
          LOGGER.debug "merge: #{gal.email} has work number #{n.to_xml} that is not in the GAL"
          n.unlink
          contact.update!
        end
      end
    }

    # add remaining GAL numbers
    numbers.each{|number|
      Nokogiri::XML::Builder.with(contact.node) do |xml|
        LOGGER.debug "merge: #{gal.email} add work number #{number}"
        xml['gd'].phoneNumber('label' => label(number, 'Work')) { xml.text(number) }
        contact.update!
      end
    }

    # add to group contacts group
    contact.node.xpath('.//gContact:groupMembershipInfo').each{|group|
      next if group['href'] == @group
      group.unlink
      contact.update!
    }
    if !contact.node.at(".//gContact:groupMembershipInfo[@href='#{@group}']")
      Nokogiri::XML::Builder.with(contact.node) do |xml|
        xml['gContact'].groupMembershipInfo(deleted: "false", href: @group)
        contact.update!
      end
    end

    # set structured name
    if OPTS.names
      contact.update!
      contact.node.xpath('.//gd:name').each{|n| n.unlink }
      Nokogiri::XML::Builder.with(contact.node) do |xml|
        xml['gd'].name {
          xml['gd'].givenName { xml.text(gal.givenName) }
          xml['gd'].familyName { xml.text(gal.familyName) }
          xml['gd'].fullName { xml.text(gal.fullName) }
        }
      end
    end
  end

  def action(a)
    return false if OPTS.offline
    return false if OPTS.dry_run
    return true if OPTS.action == ''
    return true if OPTS.action == a
    return false
  end

  def save
    saved = OpenStruct.new(updated: 0, deleted: 0, inserted: 0, retained: 0, photo: 0)

    # remove all non-entries
    @contacts.at('//xmlns:feed').children.each{|node| node.unlink unless node.name == 'entry' }

    open('update.xml', 'w'){|f| f.write(@contacts.to_xml) } unless OPTS.batch

    @contacts.xpath('//xmlns:entry').each{|contact|
      contact = Contact.new(contact)

      LOGGER.debug "#{contact.action} #{contact.corp_email}"

      case contact.action
        when :delete
          saved.deleted += 1
          contact.node.at('./xmlns:id').content = contact.node.at("./xmlns:link[@rel='self']")['href']
          contact.node.children.each{|node| node.unlink unless node.name == 'id' }

          if OPTS.batch
            Nokogiri::XML::Builder.with(contact.node) do |xml|
              xml['batch'].id { xml.text("delete-#{contact.corp_email}") }
              xml['batch'].operation(type: 'delete')
            end
          elsif action('delete')
            delete(contact.id, contact.etag)
          end

        when :insert
          saved.inserted += 1

          if OPTS.batch
            Nokogiri::XML::Builder.with(contact.node) do |xml|
              xml['batch'].id { xml.text("insert-#{contact.corp_email}") }
              xml['batch'].operation(type: 'insert')
            end
          elsif action('insert')
            post('/contacts/default/full', contact.node.dup.to_xml)
          end

        when :update
          saved.updated += 1

          contact.node.at('./xmlns:id').content = contact.node.at("./xmlns:link[@rel='self']")['href']
          if OPTS.batch
            Nokogiri::XML::Builder.with(contact.node) do |xml|
              xml['batch'].id { xml.text("update-#{contact.corp_email}") }
              xml['batch'].operation(type: 'update')
            end
          elsif action('update')
            put(contact.id, contact.node.dup.to_xml, contact.etag)
          end

        when :keep
          pic = contact.photo
          if !pic.etag && File.file?('photo.png')
            put(pic.url, open('photo.png').read, nil, 'image/png') if action('update')
            saved.photo += 1
          end

        else
          saved.retained += 1
          contact.node.unlink
      end
    }
    LOGGER.debug saved.inspect

    if OPTS.batch
      if OPTS.action != ''
        @contacts.xpath('//xmlns:entry').each_with_index{|contact, i|
          operation = contact.at('.//batch:operation')
          operation = operation['type'] if operation
          contact.unlink unless operation == OPTS.action
        }
      end
      # max 100 ops at a time
      @contacts.xpath('//xmlns:entry').each_with_index{|contact, i|
        contact.unlink if i >= 100
      }
      LOGGER.debug post('/contacts/default/full/batch', @contacts.to_xml).to_xml unless OPTS.offline || OPTS.dry_run
    end

    open('update.xml', 'w'){|f| f.write(@contacts.to_xml) } if OPTS.batch
  end
end

if !OPTS.offline
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
end

class OAB
  include Singleton

  def download(file)
    File.unlink(file) if File.file?(file)
    url = "#{@credentials.oab}/#{File.basename(file)}"
    system "wget --quiet --user=#{@credentials.user.shellescape} --password=#{@credentials.password.shellescape} -O #{file.shellescape} #{url.shellescape}"
  end

  def value(p, dflt=nil)
    return p ? (p[0] || dflt) : dflt
  end

  def initialize
    @credentials = OpenStruct.new(YAML.load_file('.exchange.yml')) unless OPTS.offline
    @updated = OPTS.offline || OPTS.force

    LOGGER.debug "loading GAL"
    @oab = OpenStruct.new({ pointer: File.expand_path(File.join(File.dirname(__FILE__), 'oab.xml')) })
    download(@oab.pointer) unless OPTS.offline
    gal = Nokogiri::XML(open(@oab.pointer))
    @oab.compressed = File.expand_path(File.join(File.dirname(__FILE__), gal.at('//Full').inner_text))
    @oab.uncompressed = @oab.compressed.sub(/\.lzx$/, '') + '.oab'
    if !File.file?(@oab.uncompressed)
      @updated = true
      throw 'cannot download in offline mode' if OPTS.offline
      clear
      download(@oab.compressed)
      decompress(@oab.compressed, @oab.uncompressed)
    end
    @oab.data = OabReader.new(@oab.uncompressed)
  end
  attr_reader :updated

  def clear
    return if OPTS.offline
    Dir[File.expand_path(File.join(File.dirname(__FILE__), '*.lzx'))].each{|lzx| File.unlink(lzx) }
    Dir[File.expand_path(File.join(File.dirname(__FILE__), '*.oab'))].each{|oab| File.unlink(oab) }
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

begin
  if OPTS.post.strip != ''
    puts GoogleContacts.instance.post('/contacts/default/full/batch', open(OPTS.post).read).to_xml
    exit
  end
  exit unless OAB.instance.updated

  # merge OAB accounts with the same email address before merging them into google contacts
  numbers = {}
  OAB.instance.each{|record|
    numbers[record.email.downcase] ||= []
    numbers[record.email.downcase].concat(record.numbers)
    record.numbers = numbers[record.email.downcase].uniq.sort
    GoogleContacts.instance.merge(record)
  }
rescue => e
  OAB.instance.clear
  raise e
end
GoogleContacts.instance.save
