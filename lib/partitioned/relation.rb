module Partitioned
  class Relation
    extend Forwardable

    attr_reader :data_array, :length, :model, :partition_key_values, :data_partitions
    def_delegators :data_array, :select, :reduce, :map, :each


    def initialize(model, partition_key_values)
      @model = model
      @partition_key_values = partition_key_values
      @data_partitions = []
      partition_key_values.each do |partition_key_value|
        @data_partitions << model.from_partition(partition_key_value)
      end
    end

    def data_array
      @data_array = @data_partitions.map(&:to_a).reduce(&:+) if @data_array.nil?
      @data_array
    end

    def length
      @data_array = @data_partitions.map(&:to_a).reduce(&:+) if @data_array.nil?
      @data_array.length
    end

    def update_all(args)
      @data_partitions.each do |part|
        part.update_all(args)
      end
    end

    def where(*args)
      new_data_partitions = []
      @data_partitions.each do |data_partition|
        new_data_partitions << data_partition.where(*args)
      end
      @data_partitions = new_data_partitions
      self
    end

    def find_by(*args)
      resource = nil
      @data_partitions.each do |data_partition|
        resource = data_partition.find_by(*args)
        return resource unless resource.nil?
      end
      resource
    end

    protected

    def method_missing(method, *args, &block)
      if @model.respond_to?(method)
        new_data_partitions = []
        @data_partitions.each do |data_partition|
          new_data_partitions << data_partition.public_send(method, *args, &block)
        end
        @data_partitions = new_data_partitions
        self
      else
        super
      end
    end
  end
end