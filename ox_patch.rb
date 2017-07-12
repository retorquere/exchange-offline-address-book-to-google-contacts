require 'ox'

module Ox
  class Element
    alias_method :super_method_missing, :method_missing

    def method_missing(id, *args, &block)
      name = id.to_s.sub('_', ':')

      if name[-1] == '!'
        name.chomp!('!')
        return self.nodes.select{|node| !node.is_a?(String) && node.name == name}
      end

      if name[-1] == '?'
        begin
          name.chomp!('?')
          return super_method_missing(name, *args, &block)
        rescue NoMethodError
          return false
        end
      end

      super_method_missing(name.to_sym, *args, &block)
    end
  end
end
