require 'rubygems' if RUBY_VERSION < '1.9.0'
require 'influxdb'
require 'timeout'

module Sensu::Extension

  class SendsToInfluxDB < Handler

    def name
      'influxdb'
    end

    def description
      'outputs metrics to InfluxDB'
    end

    def post_init
      @influxdb = InfluxDB::Client.new settings['influxdb']['database'],
          :host     => settings['influxdb']['host'],
          :port     => settings['influxdb']['port'],
          :username => settings['influxdb']['user'],
          :password => settings['influxdb']['password']
      @timeout = @settings['influxdb']['timeout'] || 15
    end

    def run(event)
      begin
        event = MultiJson.load(event)
        host = event[:client][:name]
        ip = event[:client][:address]
        _series = event[:check][:name]
        _timestamp = event[:check][:issued]
        _duration = event[:check][:duration]
        output = event[:check][:output]
      rescue => e
        @logger.error "InfluxDB: Error setting up event object - #{e.backtrace}"
      end

      begin
        points = []
        output.each_line do |metric|
          m = metric.split
          next unless m.count == 3
          series = m[0].split('.', 2)[1]
          next unless series
          series.gsub!('.', '_')
          value = m[1].to_f
          points = { host: host, ip: ip, value: value }
          begin
            @influxdb.write_point(series, points, true)
          rescue => e
            @logger.error "InfluxDB: Error posting event - #{e.backtrace}"
          end
        end
      rescue => e
        @logger.error "InfluxDB: Error parsing output lines - #{e.backtrace}"
        @logger.error "InfluxDB: #{output}"
      end

      # yield("InfluxDB: Handler finished", 0)
    end

  end
end
