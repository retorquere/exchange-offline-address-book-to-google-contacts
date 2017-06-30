require 'nokogiri'

class Nokogiri::XML::Element
  def category
    @category ||= begin
      category = self.children.detect{|field|
        field.name == 'category' && field['scheme'] == "http://schemas.google.com/g/2005#kind" && field['term']
      }
      category = category['term'].sub(/.*#/, '')
      category || :none
    end
  end

  def contacts
    raise "#{self.name} is not a contacts list" unless self.name == 'feed' && category == 'contact'
    @contacts ||= self.children.select{|contact| contact.name == 'entry'}
  end

  def groups
    raise "#{self.name} is not a groups list" unless self.name == 'feed' && category == 'group'
    @groups ||= self.children.select{|contact| contact.name == 'entry'}
  end

  def id
    raise "#{self.name} is not a contact or group" unless self.name == 'entry'
    @id ||= self.children.detect{|field| field.name == 'id'}.text
  end

  # contacts
  def email_addresses
    raise "#{self.name} is not a contact" unless self.name == 'entry'
    self.parent.contacts
    @email_addresses ||= self.children.select{|field| field.name == 'email'}.collect{|field| field['address']}
  end
  def work_email(re)
    @work_email ||= email_addresses.select{|address| address =~ re }
  end
  def private_email(re)
    @private_email ||= email_addresses.select{|address| address !~ re }
  end

  def phone_numbers
    raise "#{self.name} is not a contact" unless self.name == 'entry'
    self.parent.contacts
    @phone_numbers ||= self.children.select{|field| field.name == 'phoneNumber'}.collect{|field| field['address']}
  end
  def work_phones
    @work_phones ||= phone_numbers.select{|phone| PHONEFIELDS.include?(phone['label'].to_s.to_sym }
  end
  def private_phones
    @private_phones ||= phone_numbers.select{|phone| !PHONEFIELDS.include?(phone['label'].to_s.to_sym }
  end

  def _state
    raise "#{self.name} is not a contact" unless self.name == 'entry'
    @state ||= begin
      state = self.children.detect{|field| field.name == 'link' && field['rel'] == 'oab'}
      if state.nil?
        Nokogiri::XML::Builder.with(self){|xml| xml.link(rel: 'oab') }
        state = self.children.detect{|field| field.name == 'link' && field['rel'] == 'oab'}
      end
      state
    end
  end

  def state
    raise "#{self.name} is not a contact" unless self.name == 'entry'
    _state['href']
  end

  def state=(new_state)
    case (state || '') + ':' + new_state
      when new_state + ':' + new_state
        return
      when 'new:update'
        return
      when ':strip', ':keep', ':new', ':delete', 'delete:keep', 'delete:update', 'keep:update'
        _state['href'] = new_state
      else
        raise (state || '') + ':' + new_state + ' :: ' + self.to_xml
    end
  end
end
