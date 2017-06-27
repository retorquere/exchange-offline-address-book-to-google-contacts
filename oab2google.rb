#!/usr/bin/env ruby 

require 'signet/oauth_2/client'
require 'json'
require 'yaml'
require 'nokogiri'
require 'singleton'
require 'httparty'
require 'dotenv/load'
require 'exchange-offline-address-book'
require 'phonelib'
require 'hashie'
require 'optparse'

Options = Hashie::Mash.new
OptionParser.new do |opts|
  opts.banner = "Usage: #{File.basename(__FILE__)} [options]"

  opts.on('-c', '--config FILE', 'Config file') { |v| Options.config = v }
  opts.on('-t', '--token TOKEN', 'Google OAuth token') { |v| Options.token = v }
end.parse!

Config = Class.new(Hashie::Mash) do
  def initialize
    super

    @location = Options.config || File.join(File.dirname(__FILE__), '.config.yml')
    if File.file?(@location)
      # yaml is easier to edit, but I don't want all the type garbage
      JSON.parse(YAML::load_file(@location).to_json, object_class: Hashie::Mash).each_pair{|k, v|
        self[k] = v
      }
    end

    self.locale ||= 'NL'
    Phonelib.default_country = self.locale

    if self.exchange
      self.exchange.domain ||= self.exchange.email.gsub(/.*@/, '@').downcase if self.exchange.email && self.exchange.email =~ /@/
      self.exchange.domain ||= self.exchange.username.gsub(/.*@/, '@').downcase if self.exchange.username && self.exchange.username =~ /@/
      self.exchange.domain.downcase! if self.exchange.domain
    end

    self.save
  end
  attr_reader :location

  def save
    config = JSON.parse(self.to_json).to_yaml
    open(@location, 'w'){|f| f.write(config) }
  end
end.new

PHONEFIELDS_PRIMARY = [ :BusinessTelephoneNumber, :Business2TelephoneNumber, :MobileTelephoneNumber ]
PHONEFIELDS_ASSISTANT = [ :Assistant, :AssistantTelephoneNumber, ]
PHONEFIELDS = PHONEFIELDS_PRIMARY + PHONEFIELDS_ASSISTANT

def normalize(number)
  return nil unless Phonelib.valid?(number)
  return Phonelib.parse(number).to_s
end

def set_status(contact, status)
  case (contact['oab:status'] || '') + ':' + status
    when status + ':' + status
      return
    when 'new:update'
      return
    when ':strip', ':keep', ':new', ':delete', 'delete:keep', 'keep:update'
      contact['oab:status'] = status
    else
      raise (contact['oab:status'] || '') + ':' + status + ' :: ' + contact.to_xml
  end
end

Google = Class.new do
  class Groups
    def initialize(xml)
      open(File.join(Config.google.backup, 'groups.xml'), 'w'){|f| f.write(xml.to_xml) } if Config.google.backup

      @xml = xml
      @groups = {}
      xml.root.children.each{|group|
        next unless group.name == 'entry'
        id = nil
        name = nil
        group.children.each{|field|
          case field.name
            when 'id'
              id = field.text
            when 'title'
              name = field.text
          end
        }
        if id && name
          @groups[id] = name
          @groups[name] = id
        end
      }
      @namespaces = Hash[*xml.root.namespace_definitions.collect{|ns| ['xmlns' + (ns.prefix ? ':' + ns.prefix : ''), ns.href] }.flatten]

      raise 'Work group not set' unless Config.google.group && Config.google.group.work
      @work = @groups[Config.google.group.work] || raise("#{Config.google.group.work.inspect} not found")
      raise 'Friends group not set' unless Config.google.group && Config.google.group.friends
      @friends = @groups[Config.google.group.friends] || raise("#{Config.google.group.friends.inspect} not found")

      @starred = @groups['Starred in Android'] || raise("'Starred in Android' not found")
      @my_contacts = @groups['System Group: My Contacts'] || raise("'System Group: My Contacts' not found")
    end

    attr_reader :xml, :namespaces
    attr_reader :work, :friends, :starred, :my_contacts

    def [](id_or_name)
      @groups[id_or_name]
    end
  end

  class Contacts
    def initialize(xml)
      open(File.join(Config.google.backup, 'contacts-downloaded.xml'), 'w'){|f| f.write(xml.to_xml) } if Config.google.backup

      @xml = xml
      xml.root.add_namespace('atom', 'http://www.w3.org/2005/Atom')
      xml.root.add_namespace('oab', 'offline-address-book')

      @email = {}
      xml.root.children.each{|contact|
        next unless contact.name == 'entry'

        on_domain = nil
        off_domain = nil
        emails = []
        contact.children.each{|field|
          next unless field.name == 'email'
          email = field['address'].downcase
          @email[email] = contact
          if email.end_with?(Config.exchange.domain)
            on_domain = email
          else
            off_domain = email
          end
        }

        if off_domain && on_domain
          set_status(contact, 'strip')
        elsif on_domain
          set_status(contact, 'delete')
        end
      }

      @namespaces = Hash[*xml.root.namespace_definitions.collect{|ns| ['xmlns' + (ns.prefix ? ':' + ns.prefix : ''), ns.href] }.flatten]
      @atom = xml.root.namespace_definitions.detect{|ns| ns.prefix == 'atom'}
    end
    attr_reader :xml, :namespaces

    def [](email)
      @email[email.downcase]
    end

    def create(email)
      @email[email.downcase] ||= begin
        Nokogiri::XML::Builder.with(xml.root) do |xml|   
          xml.entry(@namespaces) {
            xml['atom'].category(scheme: "http://schemas.google.com/g/2005#kind", term: "http://schemas.google.com/contact/2008#contact")
            xml['gd'].email(label: 'HAN', address: email)
          }
        end

        contact = xml.root.children.reverse.detect{|contact|
          next unless contact.name == 'entry'
          contact.children.detect{|field|
            field.name == 'email' && field['address'].downcase == email.downcase
          }
        }
        contact.namespace = @atom
        set_status(contact, 'new')
        contact
      end
    end
  end

  def initialize
    @client = Signet::OAuth2::Client.new(
      :authorization_uri => Config.google.secrets.auth_uri,
      :token_credential_uri =>  Config.google.secrets.token_uri,
      :client_id => Config.google.secrets.client_id,
      :client_secret => Config.google.secrets.client_secret,
      :scope => 'https://www.googleapis.com/auth/contacts',
      :redirect_uri => Config.google.secrets.redirect_uris[0],
    )
  
    if Options.token
      @client.code = Options.token
      Config.google.token = @client.fetch_access_token!
      Config.save
    elsif Config.google.token
      begin
        @client.refresh_token = Config.google.token.refresh_token
        @client.fetch_access_token!
      rescue 
        Config.google.token = nil
        Config.save
      end
    end
  
    if Config.google.token.nil?
      puts @client.authorization_uri
      exit
    end

    @groups = Groups.new(request('https://www.google.com/m8/feeds/groups/default/full'))
    @contacts = Contacts.new(request('https://www.google.com/m8/feeds/contacts/default/full?max-results=10000'))
  end

  def process
    open(File.join(Config.google.backup, 'contacts-processed.xml'), 'w'){|f| f.write(@contacts.xml.to_xml) } if Config.google.backup

    @contacts.xml.root.children.each{|contact|
      next unless contact.name == 'entry'
  
      case contact['oab:status']
        when nil, 'keep'
          next

        when 'new'
          puts "new: #{contact.children.select{|field| field.name == 'email'}.collect{|field| field['address']}}"
          request('https://www.google.com/m8/feeds/contacts/default/full', method: 'POST', body: contact.to_xml)

        when 'update'
          puts "update: #{contact.children.select{|field| field.name == 'email'}.collect{|field| field['address']}}"
          link = contact.children.detect{|field| field.name == 'link' && field['rel'] == 'edit'}['href']
          request(link, method: 'PUT', body: contact.to_xml)

        when 'delete'
          puts "delete: #{contact.children.select{|field| field.name == 'email'}.collect{|field| field['address']}}"
          link = contact.children.detect{|field| field.name == 'link' && field['rel'] == 'edit'}['href']
          request(link, method: 'DELETE')

        else
          raise contact['oab:status']
      end
    }
  end

  attr_reader :groups, :contacts

  private

  def request(uri, body: '', method: 'GET')
    headers = {
      'GData-Version' =>  '3.0',
      'Content-Type' => 'application/atom+xml; charset=UTF-8; type=feed',
      'If-Match' => '*',
    }
    #case method
    #  when 'GET', 'POST' then # pass
    #  else
    #    headers['X-HTTP-Method-Override'] = method
    #    method = 'POST'
    #end
    response = @client.fetch_protected_resource(
	    method: method,
	    uri: uri,
	    headers: headers,
      body: body,
    )

    case response.status.to_i
      when 200, 201
        Nokogiri::XML(response.body)
      else
        raise "#{uri}: #{response.status.to_s}"
    end
  end
end.new

Exchange = Class.new do
  def initialize
    @oab = OfflineAddressBook.new(
      username: Config.exchange.username,
      password: Config.exchange.password,
      email: Config.exchange.email,
      cachedir: Config.exchange.cachedir || File.dirname(__FILE__),
      baseurl: Config.exchange.baseurl
    )

    @oab.records.sort_by!{|record| record.SmtpAddress ? record.SmtpAddress.downcase : '' }

    @oab.records.each{|record|
      numbers = []
      PHONEFIELDS.each{|kind|
        if !record[kind]
          record[kind] = []
        elsif record[kind].is_a?(String)
          record[kind] = [ normalize(record[kind]) ]
        else
          record[kind] = record[kind].collect{|n| normalize(n) }
        end

        record[kind].compact!
        record[kind].uniq!
        record[kind] -= numbers
        numbers += record[kind]
      }
    }

    primary_numbers = @oab.records.collect{|record|
      PHONEFIELDS_PRIMARY.collect{|kind|
        record[kind]
      }
    }.flatten.uniq

    @oab.records.each{|record|
      PHONEFIELDS_ASSISTANT.each{|kind|
        record[kind] = record[kind] - primary_numbers
      }
    }

    @oab.records.reject!{|record| PHONEFIELDS.collect{|kind| record[kind]}.flatten.empty? }

    open(@oab.cache.sub(/\.json$/, '') + '.yml', 'w') {|f| f.write(@oab.records.to_yaml) }
  end

  def records
    @oab.records
  end
end.new

Exchange.records.each{|record|
  next unless record.SmtpAddress

  contact = Google.contacts[record.SmtpAddress]

  if contact
    set_status(contact, 'keep')
  else
    contact = Google.contacts.create(record.SmtpAddress)
  end

  raise record.SmtpAddress unless contact

  PHONEFIELDS.each{|kind|
    contact.children.each{|number|
      next unless number.name == 'phoneNumber' and number['label'] == kind.to_s
      n = normalize(number.text)
      next unless n
      if record[kind].include?(n)
        record[kind].delete(n)
      else
        number.unlink
        set_status(contact, 'update')
      end
    }

    record[kind].each{|n|
      set_status(contact, 'update')
      Nokogiri::XML::Builder.with(contact) do |xml|   
        xml['gd'].phoneNumber(label: kind.to_s) { xml.text n }
      end
    }
  }

  if contact.children.select{|number| number.name == 'phoneNumber' }.empty?
    set_status(contact, 'delete')
    next
  end

  groups = {}
  contact.children.each{|group|
    next unless group.name == 'groupMembershipInfo'
    groups[:work] = group if group['href'] == Google.groups.work
    groups[:friends] = group if group['href'] == Google.groups.friends
    groups[:starred] = group if group['href'] == Google.groups.starred
    groups[:my_contacts] = group if group['href'] == Google.groups.my_contacts
  }
  if !groups[:work] && !groups[:friends]
    set_status(contact, 'update')
    Nokogiri::XML::Builder.with(contact) do |xml|   
      xml['gContact'].groupMembershipInfo(href: Google.groups.work)
    end
    groups[:starred].unlink if groups[:starred]
    groups[:my_contacts].unlink if groups[:my_contacts]
  elsif groups[:work] && groups[:friends]
    set_status(contact, 'update')
    groups[:work].unlink
  elsif groups[:work] && (groups[:starred] || groups[:my_contacts])
    set_status(contact, 'update')
    groups[:starred].unlink if groups[:starred]
    groups[:my_contacts].unlink if groups[:my_contacts]
  end

  case contact['oab:status']
    when 'new', 'update'
      fullname = record.DisplayName
      fullname = "#{$2} #{$1}".strip if fullname =~ /^(#{record.Surname}[^ ]*) (.*)/

      contact.children.each{|field|
        field.unlink if field.name == 'name' || field.name == 'title'
      }

      Nokogiri::XML::Builder.with(contact) do |xml|   
        xml.title { xml.text fullname }
        xml['gd'].name { xml['gd'].fullName { xml.text fullname } }
      end

    when 'keep'
      next

    else
      raise contact['oab:status']
  end
}

Google.process
