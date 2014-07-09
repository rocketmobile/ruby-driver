# encoding: utf-8

module Cql
  NOT_CONNECTED = NotConnectedError.new("not connected")
  SELECT_LOCAL  = Protocol::QueryRequest.new('SELECT rack, data_center, host_id, release_version FROM system.local', nil, nil, :one)
  SELECT_PEERS  = Protocol::QueryRequest.new('SELECT peer, rack, data_center, host_id, rpc_address, release_version FROM system.peers', nil, nil, :one)
  REGISTER      = Protocol::RegisterRequest.new(
                    Protocol::TopologyChangeEventResponse::TYPE,
                    Protocol::StatusChangeEventResponse::TYPE,
                    Protocol::SchemaChangeEventResponse::TYPE
                  )

  class Cluster
    class ControlConnection
      def initialize(io_reactor, request_runner, cluster_state, builder_settings)
        @io_reactor     = io_reactor
        @request_runner = request_runner
        @cluster        = cluster_state
        @settings       = builder_settings
      end

      def connect_async
        plan = @cluster.hosts.keys.to_enum
        f = connect_to_first_available(plan)
        f.on_value {|connection| @connection = connection}
        f
      end

      def register_async
        raise NOT_CONNECTED if @connection.nil?

        @request_runner.execute(@connection, REGISTER).map do
          @connection.on_event do |event|
            @settings.logger.debug('Received %s %s event' % [event.type, event.change])

            if event.type == 'SCHEMA_CHANGE'
            else
              case event.change
              when 'UP'
                address = event.address
                host    = @cluster.hosts[address.to_s]

                if host && !host.up?
                  refresh_host_async(address).map do
                    host.up!

                    @cluster.clients.each do |client|
                      client.host_up(host)
                    end
                  end.get
                end
              when 'DOWN'
                address = event.address
                host    = @cluster.hosts[address.to_s]

                if host && !host.down?
                  refresh_host_async(address).map do
                    host.down!

                    @cluster.clients.each do |client|
                      client.host_down(host)
                    end
                  end.get
                end
              when 'NEW_NODE'
                address = event.address
                host    = @cluster.hosts[address.to_s]

                refresh_host_async(address).get if host.nil?
              when 'REMOVED_NODE'
                address = event.address
                host    = @cluster.hosts.delete(address.to_s)

                if !host.nil?
                  if host.down?
                    @cluster.clients.each do |client|
                      client.host_lost(host)
                    end
                  else
                    host.down!

                    @cluster.clients.each do |client|
                      client.host_down(host)
                      client.host_lost(host)
                    end
                  end
                end
              end
            end
          end

          self
        end
      end

      def refresh_hosts_async
        raise NOT_CONNECTED if @connection.nil?

        local_ip = @connection.host
        ips      = ::Set[local_ip]

        local = @request_runner.execute(@connection, SELECT_LOCAL)
        peers = @request_runner.execute(@connection, SELECT_PEERS)

        Future.all(local, peers).map do |(local, peers)|
          populate_host(local_ip, local.first) unless local.empty?

          peers.each do |data|
            ip   = peer_ip(data)
            ips << ip
            populate_host(ip, data)
          end

          @cluster.hosts.select! {|k, _| ips.include?(k)}

          self
        end
      end

      def refresh_host_async(address)
        raise NOT_CONNECTED if @connection.nil?

        ip = address.to_s

        if ip == @connection.host
          request = @request_runner.execute(
                      @connection,
                      Protocol::QueryRequest.new(
                        'SELECT rack, data_center, host_id, release_version' \
                        'FROM system.local',
                        nil, nil, :one
                      )
                    )
        else
          request = @request_runner.execute(
                      @connection,
                      Protocol::QueryRequest.new(
                        'SELECT rack, data_center, host_id, rpc_address,' \
                        'release_version FROM system.peers WHERE peer = ?',
                        [address], nil, :one
                      )
                    )
        end

        request.map do |result|
          populate_host(ip, result.first) unless result.empty?

          self
        end
      end

      def close_async
        @connection.close
      end

      def inspect
        "#<#{self.class.name}:0x#{self.object_id.to_s(16)}>"
      end

      private

      def connect_to_first_available(plan, errors = {})
        h = plan.next
        f = connect_to_host(h)
        f.fallback do |error|
          errors[h] = error
          connect_to_first_available(plan, errors)
        end
      rescue ::StopIteration
        raise NoHostsAvailable.new(errors)
      end

      def connect_to_host(h)
        connector = Client::Connector.new([
          Client::ConnectStep.new(@io_reactor, protocol_handler_factory, @settings.port, @settings.connection_timeout, @settings.logger),
          Client::CacheOptionsStep.new,
          Client::InitializeStep.new(@settings.compressor, @settings.logger),
          authentication_step,
          Client::CachePropertiesStep.new,
        ])

        f = connector.connect(h)
        f.fallback do |error|
          if error.is_a?(QueryError) && error.code == 0x0a && @settings.protocol_version > 1
            @settings.logger.warn('Could not connect using protocol version %d (will try again with %d): %s' % [@settings.protocol_version, @settings.protocol_version - 1, error.message])
            @settings.protocol_version -= 1
            connect_to_host(h)
          else
            raise error
          end
        end
      end

      def populate_host(ip, data)
        host = @cluster.hosts[ip]

        if host.nil?
          host = @cluster.hosts[ip] = Host.new(ip, data)

          host.up!

          @cluster.clients.each do |client|
            client.host_found(host)
            client.host_up(host)
          end
        else
          host.id              = data['host_id']
          host.release_version = data['release_version']

          rack       = data['rack']
          datacenter = data['data_center']

          return if rack == host.rack && datacenter == host.datacenter

          if host.down?
            @cluster.clients.each do |client|
              client.host_lost(host)
            end

            host.rack       = rack
            host.datacenter = datacenter

            @cluster.clients.each do |client|
              client.host_found(host)
            end
          else
            host.down!

            @cluster.clients.each do |client|
              client.host_down(host)
              client.host_lost(host)
            end

            host.rack       = rack
            host.datacenter = datacenter

            host.up!

            @cluster.clients.each do |client|
              client.host_found(host)
              client.host_up(host)
            end
          end
        end
      end

      def peer_ip(data)
        ip = data['rpc_address'].to_s
        ip = data['peer'].to_s if ip == '0.0.0.0'
        ip
      end

      def protocol_handler_factory
        self.class.protocol_handler_factory(@io_reactor, @settings.protocol_version, @settings.compressor)
      end

      def authentication_step
        if @settings.protocol_version == 1
          Client::CredentialsAuthenticationStep.new(@settings.credentials)
        else
          Client::SaslAuthenticationStep.new(@settings.auth_provider)
        end
      end

      def self.protocol_handler_factory(io_reactor, protocol_version, compressor)
        lambda { |connection| Protocol::CqlProtocolHandler.new(connection, io_reactor, protocol_version, compressor) }
      end
    end
  end
end