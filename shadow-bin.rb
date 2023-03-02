# require 'forwardable'

require 'aerospike'

module TtdShadowBin
  class TtdClusterAerospike # < Aerospike::AerospikeClient
    # extend Forwardable
    # def_delegators :@client, :delete

    def initialize(host)
      @host = host
      client # force retrieval of client
    end

    def client
      @client ||= Aerospike::Client.new(@host)
    end

    def delete(corpus, set, id)
      client.delete(mk_key(corpus, set, id))
    end

    def read(corpus, set, id)
      client.get(mk_key(corpus, set, id))
    end

    def save(nsname, setname, id, data)
      wpolicy = Aerospike::WritePolicy.new
      wpolicy.record_exists_action = Aerospike::RecordExistsAction::REPLACE
      client.put(mk_key(nsname, setname, id), data, wpolicy)
    end

    def mk_key(nsname, setname, id)
      Aerospike::Key.new(nsname, setname, id)
    end
  end

  class AbstractName
    attr_reader :name

    def initialize(name)
      @name = name
    end
  end

  class TtdCorpus < AbstractName
    attr_reader :cluster

    def initialize(cluster, name)
      super(name)
      @cluster = cluster
    end

    def delete(set_name, id)
      cluster.delete(name, set_name, id)
    end

    def read(set_name, id)
      cluster.read(name, set_name, id)
    end

    def save(setname, id, data)
      cluster.save(name, setname, id, data)
    end
  end

  class TtdSet < AbstractName
    attr_reader :corpus

    def initialize(corpus, name)
      super(name)
      @corpus = corpus
    end

    # def client
    #   @corpus.client
    # end

    # def mk_key(id)
    #   Aerospike::Key.new(@namespace.name, @name, id)
    # end

    def full_name
      "#{@corpus.name}.#{name}"
    end

    def delete(id)
      corpus.delete(name, id)
    end

    def read(id)
      corpus.read(name, id)
    end

    def save(id, data)
      corpus.save(name, id, data)
    end
  end

  class TtdAttr < AbstractName
    attr_reader :set, :is_dirty
    attr_accessor :shadow_bin

    # def initialize(doc_value, set_value, attr)
    def initialize(set, attr_name)
      # @doc_value = doc_value
      @set = set
      @name = attr_name
      # @attr = attr
      is_dirty = false
    end

    def full_name
      "#{@set.name}.#{name}"
    end

    def client
      @set.client
    end
  end

  class TtdDocumentDefinition
    # extend Forwardable
    # def_delegators :@client, :delete

    attr_reader :attrs

    def initialize(*attrs)
      @attrs = Hash[*attrs]
    end

    # def delete(id)
    #   sets.collect do |s|
    #     key = set.mk_key(id)
    #     s.delete(key)
    #   end
    # end

    def []=(key, attr)
      @attrs[key] = attr
    end

    def new(id)
      doc = TtdDocumentValue.new(self, id)
      # doc.is_new = false
      doc
    end

    def read(id)
      # sets.collect{ |set| set.read(id) }
      TtdDocumentValue.new(self, id)
    end

    def delete(id)
      # iterate_sets(id) do |client, key|
      # iterate_sets(id) do |client, corpus, set, id|
      #   client.delete(key)
      # end
      sets.each do |set|
        set.delete(id)
      end
    end

    def touch(id)
      iterate_sets(id) do |client, key|
        client.touch(key)
      end
    end

    # def clients
    #   @clients ||= attrs.collect{ |b| b.client }.to_set
    # end
    def sets
      @sets ||= @attrs.values.collect{ |b| b.set }.to_set
    end

    def nested_sets
      return @nested_sets if @nested_sets

      @nested_sets = Set.new
      @attrs.values.each do |attr|
        nested_sets_aux(@nested_sets, attr.set)
      end
      @nested_sets
    end

    def nested_sets_aux(all, attr)
      all << attr.set
      attr.shadow_bin ? nested_set_aux(all, attr.shadow_bin) : all
    end

    protected

    def iterate_sets(id)
      sets.collect do |set|
        key = set.mk_key(id)
        yield set.client, key
      end
    end

    def iterate_nested_sets(id)
      nested_sets.collect do |set|
        key = set.mk_key(id)
        yield set.client, key
      end
    end

    def mk_key(id)
      Aeropsike::Key.new(ns.name, )
    end
  end




  # above are essentially metaclasses, and below are instantiated objects

  class TtdDocumentValue
    attr_reader :doc_definition, :id, :set_values

    def initialize(doc_definition, id)
      @doc_definition = doc_definition
      @id = id
      @set_values = {}
      @attr_values = {}
      @is_dirty = false
      # @attr_values = build_attrs
    end

    def build_attrs
      attr_values = doc_definition.attrs.collect do |key, attr|
        # set_value = find_set(attr.set)
        # [key, TtdAttrValue.new(self, set_value, attr)]
        [key, TtdAttrValue.new(self, attr)]
      end
      Hash[attr_values]
    end

    def find_set_value(set)
      @set_values.fetch(set) {
        # @set_values[set] = TtdSetValue.new(@doc_definition, set, false)
        @set_values[set] = TtdSetValue.new(self, set, false)
      }
      @set_values[set]
    end

    def [](key)
      a = attr_value_named(key)
      a.value
    end

    def []=(key, val)
      @is_dirty = true
      @attr_values[key].value = val
    end

    def attr_value_named(key)
      return @attr_values[key] if @attr_values.key?(key)

      @attr_values[key] = TtdAttrValue.new(self, @doc_definition.attrs.fetch(key))

      @attr_values[key]
    end

    def save
      # delete_old_data(set_val, hashes)
      @set_values.values.each do |set|
        set.save
      end
    end

    def delete_old_data(set_val, hashes)
      warn 'TODO: delete_old_data'
    end

    def to_h
      hash = {}
      doc_definition.attrs.each do |key, attr|
        hash[key] = self[key]
      end
      hash
    end

    def set_hashes
      hashes = Hash.new { |h, k| h[k] = {} }
      @attr_values.each do |attr_val|
        v = attr_val.value
        set_val = attr_val.set_value
        hashes[set_val][attr_val.name] = v
      end
    end
  end

  class TtdSetValue
    attr_accessor :db_vals

    def initialize(doc_value, set, is_new)
      @doc_value = doc_value
      @set = set
      @attr_values = []
      # if new object, don't read from the database
      @is_new = is_new
    end

    def name
      @set.name
    end

    def full_name
      @set.full_name
    end

    def read
      return nil if @has_been_read

      @has_been_read = true
      @db_vals ||= @set.read(@doc_value.id) unless @is_new
    end

    def values
      @values ||=
      if @db_vals
        @db_vals.bins  # TODO: this should not be aerospike specific
      else
        {}
      end
    end

    def save
      attrs = pull_attr_values
      attrs = values
      @set.save(@doc_value.id, attrs)
    end

    def pull_attr_values
      @attr_values.each do |attr|
        values[attr.name] = attr.value
      end
    end

    def add_attr_value(attr_value)
      @attr_values << attr_value
    end
  end

  class TtdAttrValue
    attr_accessor :is_dirty, :extract_func, :shadow_bin, :attr, :is_shadow_bin

    def initialize(doc_value, attr)
      @doc_value = doc_value
      @attr = attr
      @is_dirty = false
      @extract_func = nil # for protobuf, compression, etc.
      if attr.shadow_bin
        @shadow_bin = TtdAttrValue.new(doc_value, attr.shadow_bin)
        @shadow_bin.is_shadow_bin = true
      end
    end

    def name
      @attr.name
    end

    def full_name
      @attr.full_name
    end

    def value
      @value = get_value unless @value_was_gotten
      @value
    end

    def value=(val)
      @value_was_gotten = true
      @value = val
    end

    def shadowed_value
      unless @value_was_gotten
        @value = get_value
        set_value.values.delete(@attr.name)
      end
      @value
    end

    def get_value
      set_value.read
      v = if @value
            @value
          elsif set_value.values.key?(@attr.name)
            set_value.values[@attr.name]
          elsif shadow_bin
            @using_shadow = true
            shadow_bin.shadowed_value
          end
      @value_was_gotten = true
      v
    end

    def set_value
      return @set_value if @set_value

      @set_value = @doc_value.find_set_value(@attr.set)
      @set_value.add_attr_value(self) unless @is_shadow_bin
      @is_dirty = true
      @set_value
    end
  end
end
