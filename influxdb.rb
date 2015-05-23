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
        series = event[:check][:name]
        timestamp = event[:check][:issued]
        duration = event[:check][:duration]
        output = event[:check][:output]
      rescue => e
        @logger.error "InfluxDB: Error setting up event object - #{e.backtrace}"
      end

      begin
        points = []
        output.each_line do |line|
          @logger.debug("Parsing line: #{line}")
          k,v,t = line.split(/\s+/)
          v = v.match('\.').nil? ? Integer(v) : Float(v) rescue v.to_s

          if @settings['influxdb']['strip_metric']
            k.gsub!(/^.*#{@settings['influxdb']['strip_metric']}\.(.*$)/, '\1')
          end

          points << {:time => t.to_f, :host => host, :metric => k, :value => v}
        end
      rescue => e
        @logger.error "InfluxDB: Error parsing output lines - #{e.backtrace}"
        @logger.error "InfluxDB: #{output}"
      end

      begin
        @influxdb.write_point(series, points, true)
      rescue => e
        @logger.error "InfluxDB: Error posting event - #{e.backtrace}"
      end
      yield("InfluxDB: Handler finished", 0)
    end

  end
end
