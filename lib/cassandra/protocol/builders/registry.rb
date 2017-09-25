# encoding: utf-8

#--
# Copyright 2013-2017 DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#++

module Cassandra
  module Protocol
    module Builders
      # @private
      class Registry
        include MonitorMixin

        def initialize(default_builder)
          @builders = ::Hash.new

          mon_initialize
          self.default = default_builder
        end

        def fetch(keyspace, identifier)
          @builders[determine_key(keyspace, identifier)]
        end

        def add(keyspace, identifier, builder)
          validate_builder(builder)
          key = determine_key(keyspace, identifier)

          synchronize do
            builders = @builders.dup
            builders[key] = builder
            @builders = builders
          end

          self
        end

        def remove(keyspace, identifier)
          key = determine_key(keyspace, identifier)

          synchronize do
            builders = @builders.dup
            builders.delete(key)
            @builders = builders
          end

          self
        end

        def default=(builder)
          validate_builder(builder)

          synchronize do
            @builders.default = builder
          end

          builder
        end

        private

        def determine_key(keyspace, identifier)
          "#{keyspace}.#{identifier}"
        end

        def validate_builder(builder)
          unless valid_builder?(builder)
            raise ::ArgumentError,
              "#{builder} is not a valid builder class. " \
              'Builders must respond to .new, #[]=, and #build'
          end
        end

        def valid_builder?(builder)
          builder.respond_to?(:new) &&
          builder.method_defined?(:[]=) &&
          builder.method_defined?(:build)
        end
      end
    end
  end
end
