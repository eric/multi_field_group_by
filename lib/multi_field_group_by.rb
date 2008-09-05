module ActiveRecord
  module Calculations 
    
#    def self.included(base)
#      base.extend(ClassMethods)
#    end
    module ClassMethods
      
      
      protected
      
      def construct_calculation_sql(operation, column_name, options) #:nodoc:
        operation = operation.to_s.downcase
        options = options.symbolize_keys
        
        scope           = scope(:find)
        merged_includes = merge_includes(scope ? scope[:include] : [], options[:include])
        aggregate_alias = column_alias_for(operation, column_name)
        column_name     = "#{connection.quote_table_name(table_name)}.#{column_name}" if column_names.include?(column_name.to_s)
        
        if operation == 'count'
          if merged_includes.any?
            options[:distinct] = true
            column_name = options[:select] || [connection.quote_table_name(table_name), primary_key] * '.'
          end
          
          if options[:distinct]
            use_workaround = !connection.supports_count_distinct?
          end
        end
        
        if options[:distinct] && column_name.to_s !~ /\s*DISTINCT\s+/i
          distinct = 'DISTINCT ' 
        end
        sql = "SELECT #{operation}(#{distinct}#{column_name}) AS #{aggregate_alias}"
        
        # A (slower) workaround if we're using a backend, like sqlite, that doesn't support COUNT DISTINCT.
        sql = "SELECT COUNT(*) AS #{aggregate_alias}" if use_workaround
        
        if options[:groups]
          options[:groups].each do |group|
            sql << ", #{group[:field]} AS #{group[:alias]}"
          end
        end
        
        sql << " FROM (SELECT #{distinct}#{column_name}" if use_workaround
        sql << " FROM #{connection.quote_table_name(table_name)} "
        if merged_includes.any?
          join_dependency = ActiveRecord::Associations::ClassMethods::JoinDependency.new(self, merged_includes, options[:joins])
          sql << join_dependency.join_associations.collect{|join| join.association_join }.join
        end
        add_joins!(sql, options, scope)
        add_conditions!(sql, options[:conditions], scope)
        add_limited_ids_condition!(sql, options, join_dependency) if join_dependency && !using_limitable_reflections?(join_dependency.reflections) && ((scope && scope[:limit]) || options[:limit])
        
        if options[:groups]
          group_key = connection.adapter_name == 'FrontBase' ?  lambda {|g| g[:alias]} : lambda {|g| g[:field]}
          sql << " GROUP BY #{options[:groups].map(&group_key).join(', ')} "
        end
        
        if options[:group] && options[:having]
          # FrontBase requires identifiers in the HAVING clause and chokes on function calls
          if connection.adapter_name == 'FrontBase'
            options[:having].downcase!
            options[:having].gsub!(/#{operation}\s*\(\s*#{column_name}\s*\)/, aggregate_alias)
          end
          
          sql << " HAVING #{options[:having]} "
        end
        
        sql << " ORDER BY #{options[:order]} "       if options[:order]
        add_limit!(sql, options, scope)
        sql << ')' if use_workaround
        sql
      end
      
      def execute_grouped_calculation(operation, column_name, column, options) #:nodoc:
        groups          = []
        aggregates      = []
        options[:group] = options[:group].split(',') if options[:group].is_a?(String)
        options[:group] = [options[:group]] unless options[:group].is_a?(Array)
        aggregate_alias = column_alias_for(operation, column_name)
        
        options[:group].each do |group_option|
          group_attr     = group_option.to_s.strip
          association    = reflect_on_association(group_attr.to_sym)
          associated     = association && association.macro == :belongs_to # only count belongs_to associations
          group_field    = associated ? association.primary_key_name : group_attr
          group_alias    = column_alias_for(group_field)
          group_column   = column_for(group_field)
          groups << {
            :column      => group_column, 
            :field       => group_field, 
            :alias       => group_alias,
            :association => association,
            :associated  => associated
          }
          aggregates << {:alias => column_alias_for(operation, column_name)}
        end
        
        sql             = construct_calculation_sql(operation, column_name, options.merge(:groups => groups))
        calculated_data = connection.select_all(sql)
        
        groups.each do |group|
          if group[:association]
            key_ids     = calculated_data.collect { |row| row[group[:alias]] } 
            key_records = group[:association].klass.base_class.find(key_ids)
            group[:key_records] = key_records.inject({}) { |hsh, r| hsh.merge(r.id => r) }
          end
        end
        
        calculated_data.inject(ActiveSupport::OrderedHash.new) do |all, row|
          key = groups.collect do |group|
            key = type_cast_calculated_value(row[group[:alias]], group[:column])
            key = group[:key_records][key] if group[:key_records]
            key
          end
          key = key.first if key.length == 1
          value = row[aggregate_alias]
          all[key] = type_cast_calculated_value(value, column, operation)
          all
        end
      end
      
    end
  end
end
