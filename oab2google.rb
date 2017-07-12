#!/usr/bin/env ruby

require 'signet/oauth_2/client'
require 'json'
require 'yaml'
require 'ox'
require_relative 'ox_patch'
require 'singleton'
require 'httparty'
require 'dotenv/load'
require 'exchange-offline-address-book'
require 'phonelib'
require 'hashie'
require 'optparse'
require 'mime/types'

Options = Class.new(Hashie::Dash) do
  property :config
  property :token
  property :force
  property :force
  property :force_photo
  property :remove
  property :dry_run
  property :verbose
  property :proceed
  property :number
end.new
OptionParser.new do |opts|
  opts.banner = "Usage: #{File.basename(__FILE__)} [options]"

  opts.on('-c', '--config FILE', 'Config file') { |v| Options.config = v }
  opts.on('-t', '--token TOKEN', 'Google OAuth token') { |v| Options.token = v }
  opts.on('-F', '--force', 'Force-update all contacts') { |v| Options.force = true }
  opts.on('-f', '--force-photo', 'Force-update photos on all contacts') { |v| Options.force_photo = true }
  opts.on('-r', '--remove', 'Remove all work contacts from Google contacts') { |v| Options.remove = true }
  opts.on('-d', '--dry-run', "Do anything but send changes to Google") { |v| Options.dry_run = true }
  opts.on('-v', '--verbose', "Be chatty") { |v| Options.verbose = true }
  opts.on('-p', '--proceed', "Proceed on API errors") { |v| Options.proceed = true }
  opts.on('-n', '--number N', "Only affect N records") { |v| Options.number = Integer(v) }
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

    self.exchange.domain ||= self.exchange.email.gsub(/.*@/, '').downcase if self.exchange.email && self.exchange.email =~ /@/
    self.exchange.domain ||= self.exchange.username.gsub(/.*@/, '').downcase if self.exchange.username && self.exchange.username =~ /@/
    self.exchange.domain.downcase!
    raise "Domain not set" if self.exchange.domain.to_s == ''

    raise "Photo #{self.photo} not found" if self.photo && !File.file?(self.photo)

    self.save
  end
  attr_reader :location

  def save
    config = JSON.parse(self.to_json).to_yaml
    open(@location, 'w'){|f| f.write(config) }
  end
end.new

EXCHANGE_DOMAIN = /[@\.]#{Config.exchange.domain}$/i

PHONEFIELDS_PRIMARY = [ :BusinessTelephoneNumber, :Business2TelephoneNumber, :MobileTelephoneNumber ]
PHONEFIELDS_ASSISTANT = [ :Assistant, :AssistantTelephoneNumber, ]
PHONEFIELDS = PHONEFIELDS_PRIMARY + PHONEFIELDS_ASSISTANT

def normalize(number)
  return nil unless Phonelib.valid?(number)
  return Phonelib.parse(number).international
end

GoogleAccount = Class.new do
  class Contact
    if Config.photo
      @@photo_mime_type = MIME::Types.type_for(Config.photo)[0].to_s
      @@photo = open(Config.photo).read
    end

    def initialize(node)
      @node = node
      @groups = {}
      @phones = {}
      PHONEFIELDS.each{|field| @phones[field] = [] }

      node.nodes.each{|field|
        next if field.is_a?(String)

        case field.name

        when 'id'
          @id = field.text

        when 'title'
          @title = field.text

        when 'link'
          case field.rel
          when 'http://schemas.google.com/contacts/2008/rel#photo'
            @photo = field.href
            @has_photo = field['gd:etag']

          when 'edit'
            @edit = field.href

          when 'self'
            # ignore

          else
            raise field.name # Ox.dump(node)
          end

        when 'gd:name'
          @fullName = field.gd_fullName.text if field.gd_fullName?

        when 'gd:email'
          @work = field.address if field.address =~ EXCHANGE_DOMAIN

        when 'gd:phoneNumber'
          @phones[field.label.to_sym] << field.text if field['label'] && PHONEFIELDS.include?(field.label.to_sym)

        when 'gContact:groupMembershipInfo'
          @groups[field.href] = true if !field['deleted'] || field.deleted == 'false'

        when 'gd:organization'
          @organization = field.gd_orgName.text if field.gd_orgName?

        when 'updated', 'category', 'content'

        when 'app:edited'

        when 'atom:category'

        when 'gd:structuredPostalAddress', 'gd:extendedProperty', 'gd:im'

        when 'gContact:birthday', 'gContact:fileAs', 'gContact:website', 'gContact:userDefinedField'

        else
          raise field.name # Ox.dump(node)

        end
      }

      self.status = @edit ? :delete : :new # assume a work contact needs to be deleted unless later marked present
    end

    attr_reader :status, :fullName, :node, :work, :photo, :edit, :groups, :organization, :id, :title

    def photo?
      @has_photo
    end

    def status=(status)
      transition = "#{@status}:#{status}"

      case transition
      when "#{status}:#{status}"

      when 'new:update'

      when ':strip', ':delete'
        @status = status.to_sym

      when ':keep', ':new', 'delete:keep', 'delete:update', 'keep:update'
        @status = status.to_sym unless Options.remove

      else
        raise transition + ' :: ' + self.work
      end
    end

    def merge(contact)
      puts "Merging #{contact.SmtpAddress}" if Options.verbose
      self.status = :keep if @status == :delete

      if !@node.gd_organization? || !@node.gd_organization.gd_orgName? || @node.gd_organization.gd_orgName.text != Config.google.group.work
        puts "  Added company #{@work}: #{Config.google.group.work}" if Options.verbose
        self.status = :update
        @node.nodes.delete_if{|field|
          field.name == 'gd:organization'
        }
        org = Ox::Element.new('gd:organization')
        org['rel'] = 'http://schemas.google.com/g/2005#other'
        org << Ox::Element.new('gd:orgName')
        org.gd_orgName << Config.google.group.work
        @node.nodes << org
      end

      # remove old numbers
      @node.nodes.delete_if{|field|
        if field.name != 'gd:phoneNumber' || !field['label'] || !PHONEFIELDS.include?(field.label.to_sym)
          false
        elsif contact[field.label.to_sym].include?(field.text)
          false
        else
          puts "  Removed #{@work} #{field.text}" if Options.verbose
          self.status = :update
          true
        end
      }

      # add new numbers
      PHONEFIELDS.each{|kind|
        (contact[kind] - @phones[kind]).each{|number|
          n = Ox::Element.new('gd:phoneNumber')
          n['label'] = kind.to_s
          n << number
          @node << n
          puts "  Added #{@work} #{number}"
          self.status = :update
        }
      }

      if @node.gd_phoneNumber!.empty?
        self.status = :delete
        return
      end

      work = GoogleAccount.groups.work
      friends = GoogleAccount.groups.friends
      starred = GoogleAccount.groups.starred
      my_contacts = GoogleAccount.groups.my_contacts
      if !@groups[work] && !@groups[friends]
        # no groups assigned, assign to work
        puts "  Added #{@work} to work"
        self.status = :update

        group = Ox::Element.new('gContact:groupMembershipInfo')
        group['href'] = GoogleAccount.groups.work
        @node.nodes.delete_if{|field|
          field.name == 'gContact:groupMembershipInfo' && field['href'] && [starred, my_contacts].include?(field['href'])
        }
      elsif @groups[work] && @groups[friends]
        puts "  Removed #{@work} from work"
        self.status = :update
        @node.nodes.delete_if{|field|
          field.name == 'gContact:groupMembershipInfo' && field['href'] == work
        }
      elsif @groups[work] && (@groups[starred] || @groups[my_contacts])
        puts "  Un#{@groups[starred] ? 'starred' : 'mine-d'} #{@work}"
        self.status = :update
        @node.nodes.delete_if{|field|
          field.name == 'gContact:groupMembershipInfo' && field['href'] && [starred, my_contacts].include?(field['href'])
        }
      end

      case @status
      when :new, :update
          fullname = contact.DisplayName
          fullname = "#{$2} #{$1}".strip if fullname =~ /^(#{contact.Surname}[^ ]*) (.*)/
          puts "  #{@status} #{@work} to name #{fullname}"
          self.status = :update
          @node.nodes.delete_if{|field|
            %w{gd:name title}.include?(field.name)
          }

          name = Ox::Element.new('title')
          name << fullname
          @node.nodes << name

          name = Ox::Element.new('gd:name')
          name << Ox::Element.new('gd:fullName')
          name.gd_fullName << fullname
          @node.nodes << name

      when :keep

      else
        raise @status.inspect
      end
    end

    def xml
      Ox.dump(self.node)
    end

    def save
      # do patch & new & delete magic here
      case @status
        when :keep
          return false

        when :new
          puts "new: #{@work}" if Options.verbose
          GoogleAccount.request('https://www.google.com/m8/feeds/contacts/default/full', method: 'POST', body: Ox.dump(@node))

        when :update
          puts "update: #{@work}" if Options.verbose
          return false unless GoogleAccount.request(@edit, method: 'PUT', body: Ox.dump(@node))

        when :delete
          if @edit
            puts "delete: #{@work}" if Options.verbose
            GoogleAccount.request(@edit, method: 'DELETE')
          end

        else
          raise "#{@status}: #{@work}"
      end

      if Config.photo && self.photo && (!self.photo? || Options.force || Options.force_photo)
        puts "add photo: #{self.work}" if Options.verbose
        GoogleAccount.request(self.photo, method: 'PUT', content_type: @@photo_mime_type, body: @@photo)
      end

      return true
    end
  end

  class Contacts
    def initialize(xml)
      open(File.join(Config.google.backup, 'contacts.xml'), 'w'){|f| f.write(xml) } if Config.google.backup
      doc = Ox.parse(xml)

      @contacts = {}
      entry = nil;
      doc.root.entry!.each{|node|
        @contacts[entry.work.downcase] = entry if entry && entry.work
        entry = Contact.new(node)
      }
      @contacts[entry.work.downcase] = entry if entry && entry.work
    end
    attr_reader :contacts

    def [](email)
      @contacts[email.downcase] ||= begin
        node = Ox::Element.new('entry')
        node << Ox::Element.new('atom:category')
        node.atom_category['scheme'] = 'http://schemas.google.com/g/2005#kind'
        node.atom_category['term'] = 'http://schemas.google.com/contact/2008#contact'
        node << Ox::Element.new('gd:email')
        node.gd_email['label'] = Config.google.group.work
        node.gd_email['address'] = email

        contact = Contact.new(node)

        contact
      end
    end
  end

  class Group
    def initialize(node)
      @node = node
      node.nodes.each{|field|
        return if field.is_a?(String)

        case field.name
        when 'id'
          @id = field.text

        when 'title'
          @title = field.text

        when 'updated', 'category', 'content', 'link', 'gContact:systemGroup', 'app:edited'

        else
          raise 'group:' + field.name # Ox.dump(field)

        end
      }
    end

    attr_reader :id, :title, :node
  end
  class Groups
    def initialize(xml)
      open(File.join(Config.google.backup, 'groups.xml'), 'w'){|f| f.write(xml) } if Config.google.backup
      doc = Ox.parse(xml)

      @groups = {}
      entry = nil
      doc.root.entry!.each{|node|
        @groups[entry.id] = @groups[entry.title] = entry.id if entry
        entry = Group.new(node)
      }
      @groups[entry.id] = @groups[entry.title] = entry.id if entry

      raise 'Work group not set' unless Config.google.group && Config.google.group.work
      @work = @groups[Config.google.group.work] || raise("Contact group #{Config.google.group.work.inspect} not found in #{@groups.keys.inspect}")

      if Config.google.group.friends
        @friends = @groups[Config.google.group.friends] || raise("Contact group #{Config.google.group.friends.inspect} not found")
      end

      @starred = @groups['Starred in Android'] || raise("Contact group 'Starred in Android' not found")
      @my_contacts = @groups['System Group: My Contacts'] || raise("Contact group 'System Group: My Contacts' not found")
    end
    attr_reader :groups
    attr_reader :work, :friends, :starred, :my_contacts

    def [](id)
      @groups[id]
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

    @groups = Groups.new(request('https://www.google.com/m8/feeds/groups/default/full')) || exit
    @contacts = Contacts.new(request('https://www.google.com/m8/feeds/contacts/default/full?max-results=10000')) || exit
  end


  def process
    raise 'to do'
    open(File.join(Config.google.backup, 'contacts-processed.xml'), 'w'){|f| f.write(@contacts.xml.to_xml) } if Config.google.backup

    @contacts.xml.root.children.each{|contact|
      next unless contact.name == 'entry'

      case contact['oab:status']
        when nil
          next

        when 'keep'
          add_photo(contact)

        when 'new'
          puts "new: #{get_email(contact)}" if Options.verbose
          request('https://www.google.com/m8/feeds/contacts/default/full', method: 'POST', body: contact.to_xml)

        when 'update'
          puts "update: #{get_email(contact)}" if Options.verbose
          link = contact.children.detect{|field| field.name == 'link' && field['rel'] == 'edit'}['href']
          request(link, method: 'PUT', body: contact.to_xml) && add_photo(contact)

        when 'delete'
          puts "delete: #{get_email(contact)}" if Options.verbose
          link = contact.children.detect{|field| field.name == 'link' && field['rel'] == 'edit'}['href']
          request(link, method: 'DELETE')

        else
          raise contact['oab:status'] + ' :: ' + contact.to_xml
      end
    }
  end

  attr_reader :groups, :contacts

  def request(uri, body: '', method: 'GET', content_type: 'application/atom+xml; charset=UTF-8; type=feed')
    if Options.dry_run && method != 'GET'
      puts "Dry run -- not sending #{method} request to #{uri}"
      return nil
    end

    headers = {
      'GData-Version' =>  '3.0',
      'Content-Type' => content_type,
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
        return response.body
      else
        msg = "#{uri}: #{response.status} #{response.headers} #{response.body}"
        if Options.proceed
          puts msg
          return false
        else
          raise msg
        end
    end
  end
end.new

ExchangeOAB = Class.new do
  def initialize
    puts 'Loading OAB...' if Options.verbose
    @oab = Exchange::OfflineAddressBook::AddressBook.new(
      username: Config.exchange.username,
      password: Config.exchange.password,
      email: Config.exchange.email,
      cachedir: Config.exchange.cachedir || File.dirname(__FILE__),
      baseurl: Config.exchange.baseurl
    )
    puts 'OAB loaded' if Options.verbose

    @cache = @oab.cache.gsub(/\.json$/, '') + '.pruned.json'

    if File.file?(@cache)
      @oab.load(@cache)
    else
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
      puts 'OAB pruned' if Options.verbose

      @oab.save(@cache)
    end
  end

  def records
    @oab.records
  end
end.new

saved = 0
ExchangeOAB.records.each{|record|
  next if Options.remove
  next unless record.SmtpAddress

  raise record.SmtpAddress unless record.SmtpAddress =~ EXCHANGE_DOMAIN
  contact = GoogleAccount.contacts[record.SmtpAddress]
  contact.merge(record)
  puts "Merged #{record.SmtpAddress}, status: #{contact.status}" if Options.verbose
  if contact.save
    saved += 1
    exit if !Options.number.nil? && saved >= Options.number
  end
}
