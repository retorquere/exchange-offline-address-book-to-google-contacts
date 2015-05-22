#!/usr/bin/env ruby

require 'base64'
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
  opt :group,   "Contacts group", :type => :string, :default => ''
  opt :log,     "Log level", :type => :string, :default => 'warn'
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
    if OPTS.group.to_s.strip == ''
      @group = groups.at("//xmlns:entry[gContact:systemGroup[@id='Coworkers']]/xmlns:id/text()").to_xml
    else
      @group = groups.at("//xmlns:entry[xmlns:title/text()='#{OPTS.group}']/xmlns:id/text()").to_xml
    end

    if OPTS.offline
      @contacts = Nokogiri::XML(open('contacts.xml'))
    else
      @contacts = get("/contacts/default/full?max-results=10000")
      open('contacts.xml', 'w'){|f| f.write(@contacts.to_xml) }
    end
    @contact = {}
    @contacts.xpath('//xmlns:entry').each{|contact|
      email = corp_email(contact)
      if email
        @contact[email.downcase] = contact
      else
        contact.xpath('.//gContact:groupMembershipInfo').each{|group|
          LOGGER.warn "#{contact.at('.//xmlns:title').inner_text} in coworkers" if group['href'] == @group
        }
        contact.unlink
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

  def corp_email(contact)
    return contact.xpath('.//gd:email').collect{|address| address['address'] }.compact.collect{|address| address.downcase }.detect{|address| address =~ /@#{OPTS.domain}$/i }
  end

  def merge(contact)
    return if contact.email.to_s == ''

    @status ||= {}
    @status[contact.email.downcase] ||= OpenStruct.new
    status = @status[contact.email.downcase]

    LOGGER.debug "merging #{contact.email}"
    gcontact = @contact[contact.email.downcase]
    if gcontact
      LOGGER.debug "merge: #{contact.email} found"
      status.xml = contact.to_xml unless status.xml
      status.action = :update

      numbers = contact.numbers.dup
      gcontact.xpath('.//gd:phoneNumber').each{|n|
        number = telephone(n.inner_text)
        if numbers.include?(number)
          # make sure work numbers are labeled as such
          LOGGER.debug "#{contact.email}: work number #{number}"
          n['label'] = label(number, 'Work')
          numbers.delete(number)
        else # remove work-labeled numbers that are not in the GAL
          if n['label'] =~ /work/i
            LOGGER.debug "merge: #{contact.email} has work number #{n.to_xml} that is not in the GAL"
            n.unlink
          end
        end
      }

      # add remaining GAL numbers
      numbers.each{|number|
        Nokogiri::XML::Builder.with(gcontact) do |xml|
          LOGGER.debug "merge: #{contact.email} add work number #{number}"
          xml['gd'].phoneNumber('label' => label(number, 'Work')) { xml.text(number) }
        end
      }

      # add to group contacts group
      gcontact.xpath('.//gContact:groupMembershipInfo').each{|group|
        group.unlink unless group['href'] == @group
      }
      if !gcontact.at(".//gContact:groupMembershipInfo[@href='#{@group}']")
        Nokogiri::XML::Builder.with(gcontact) do |xml|
          xml['gContact'].groupMembershipInfo(deleted: "false", href: @group)
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
      LOGGER.debug "merge: new contact #{contact.email}"
      status.action = :insert

      Nokogiri::XML::Builder.with(@contacts.at('//xmlns:feed')) do |xml|
        xml.entry('xmlns:gd' => "http://schemas.google.com/g/2005", 'xmlns:gContact' => "http://schemas.google.com/contact/2008") {
          xml.category('scheme'=>"http://schemas.google.com/g/2005#kind", 'term'=>"http://schemas.google.com/contact/2008#contact")
          xml.title('type' => 'text') { xml.text(contact.fullName) }
          xml['gd'].organization('rel' => "http://schemas.google.com/g/2005#work", 'primary' => "true") {
            xml['gd'].orgName { xml.text(OPTS.domain.gsub(/\..*/, '')) }
          }
          xml['gd'].email(rel: 'http://schemas.google.com/g/2005#work', address: contact.email)
          contact.numbers.each{|number|
            xml['gd'].phoneNumber(label: label(number, 'Work')) { xml.text(number) }
          }
          xml['gContact'].groupMembershipInfo(deleted: "false", href: @group)
          xml['gd'].name {
            xml['gd'].givenName { xml.text(contact.givenName) }
            xml['gd'].familyName { xml.text(contact.familyName) }
            xml['gd'].fullName { xml.text(contact.fullName) }
          }
        }
      end
    end
  end

  def photo(contact)
    photo = gcontact.at("./xmlns:link[@rel='http://schemas.google.com/contacts/2008/rel#photo']")
    return false unless photo
    return OpenStruct.new(url: photo['href'], etag: photo['gd:etag'])
  end

  def action(a)
    return false if OPTS.offline
    return false if OPTS.dry_run
    retrurn true if OPTS.action == ''
    retrurn true if OPTS.action == a
    return false
  end

  def save
    saved = OpenStruct.new(updated: 0, deleted: 0, inserted: 0, retained: 0)

    @contacts.at('//xmlns:feed').children.each{|node| node.unlink unless node.name == 'entry' }

    @contacts.xpath('//xmlns:entry').each{|contact|
      id = corp_email(contact)
      throw contact.to_xml if !id
      status = @status[id.downcase]

      if !status
        LOGGER.debug "#{id} not in GAL"
        status = OpenStruct.new(action: :delete)
      end

      if contact.xpath('.//gd:phoneNumber').length == 0
        LOGGER.debug "#{id} has no numbers"
        status.action = :delete
      end

      status.action = :keep if status.action == :update && status.xml == contact.to_xml
      status.action ||= :ignore

      LOGGER.debug "#{status.action} #{id}"

      case status.action
        when :delete
          saved.deleted += 1
          contact.at('./xmlns:id').content = contact.at("./xmlns:link[@rel='self']")['href']
          contact.children.each{|node| node.unlink unless node.name == 'id' }

          if OPTS.batch
            Nokogiri::XML::Builder.with(contact) do |xml|
              xml['batch'].id { xml.text("delete-#{id}") }
              xml['batch'].operation(type: 'delete')
            end
          elsif action('delete')
            delete(contact.at('./xmlns:id').inner_text, contact['gd:etag'])
          end

        when :insert
          saved.inserted += 1

          if OPTS.batch
            Nokogiri::XML::Builder.with(contact) do |xml|
              xml['batch'].id { xml.text("insert-#{id}") }
              xml['batch'].operation(type: 'insert')
            end
          elsif action('insert')
            post('/contacts/default/full', contact.dup.to_xml)
          end

        when :update
          saved.updated += 1

          contact.at('./xmlns:id').content = contact.at("./xmlns:link[@rel='self']")['href']
          if OPTS.batch
            Nokogiri::XML::Builder.with(contact) do |xml|
              xml['batch'].id { xml.text("update-#{id}") }
              xml['batch'].operation(type: 'update')
            end
          elsif action('update')
            put(contact.at('./xmlns:id').inner_text, contact.dup.to_xml, contact['gd:etag'])
          end

        when :keep
          if action('update')
            pic = photo(contact)
            put(pic.url, open('photo.png').read, nil, 'image/png') if !pic.etag && File.file?('photo.png')
          end

        else
          saved.retained += 1
          contact.unlink
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

    open('update.xml', 'w'){|f| f.write(@contacts.to_xml) }
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

    LOGGER.debug "loading GAL"
    @oab = OpenStruct.new({ pointer: File.expand_path(File.join(File.dirname(__FILE__), 'oab.xml')) })
    download(@oab.pointer) unless OPTS.offline
    gal = Nokogiri::XML(open(@oab.pointer))
    @oab.compressed = File.expand_path(File.join(File.dirname(__FILE__), gal.at('//Full').inner_text))
    @oab.uncompressed = @oab.compressed.sub(/\.lzx$/, '') + '.oab'
    if !File.file?(@oab.uncompressed)
      throw 'cannot download in offline mode' if OPTS.offline
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

gc = GoogleContacts.new
if OPTS.post != ''
  puts gc.post('/contacts/default/full/batch', open(OPTS.post).read).to_xml
  exit
end

oab = OAB.new
numbers = {}
oab.each{|record|
  numbers[record.email.downcase] ||= []
  numbers[record.email.downcase].concat(record.numbers)
  record.numbers = numbers[record.email.downcase].uniq.sort
  gc.merge(record)
}
gc.save
