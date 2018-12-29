require "influxdb"
require 'thread'
require 'prometheus/client'
require 'net/http'

require_relative 'prometheus_push'

module Executor

    class DatabaseLoger

        def initialize(database_url, prometheus_gateway_url, id,jobId,procId,hfId,wfid,jobExecutable)
            @id = id
            @jobId = jobId
            @hfId = hfId
            @wfid = wfid
            @procId =procId

            @stage=jobExecutable

            @curentStage = "idle"
            @subStage = "idle"

            @startTime = nil
            @stagesTime = nil
            @stageStartTime =nil

            @influxdb = nil

            @registry = Prometheus::Client.registry
            @prometheus_pushGateway = Prometheus::Client::Push.new('amqp-executor', id, prometheus_gateway_url)

            @execution_times_running_time_gauge = Prometheus::Client::Gauge.new(:execution_times_running_time, 'execution_times_running_time')
            @execution_times_downloading_time_gauge = Prometheus::Client::Gauge.new(:execution_times_downloading_time, 'execution_times_downloading_time')
            @execution_times_execution_time_gauge = Prometheus::Client::Gauge.new(:execution_times_execution_time, 'execution_times_execution_time')
            @execution_times_uploading_time_gauge = Prometheus::Client::Gauge.new(:execution_times_uploading_time, 'execution_times_uploading_time')

            @registry.register(@execution_times_running_time_gauge)
            @registry.register(@execution_times_downloading_time_gauge)
            @registry.register(@execution_times_execution_time_gauge)
            @registry.register(@execution_times_uploading_time_gauge)

            @prometheus_pushGateway.add(@registry)

            if database_url.nil? || database_url.eql?("")
                @influxdb = nil
            else
                @influxdb = InfluxDB::Client.new url: database_url
            end
        end

        # def self.start_ecutiontimer(database_url,id,hfId,wfid)
        #     @@downloadtimer[id]=Time.now
        # end

        def changeStageAndSubStage(stage,subStage)
            self.write_data("execution_log", @curentStage,"finish")
            self.write_data("execution_log_sub_stage", @subStage,"finish")

            @curentStage = stage
            @subStage = subStage

            self.write_data("execution_log_sub_stage", @subStage,"start")
            self.write_data("execution_log", @curentStage,"start")
        end

        def changeSubStage(subStage)
            #self.write_data("execution_log_sub_stage", @subStage,"finish")

            @subStage = subStage

            self.write_data("execution_log_sub_stage", @subStage,"start")
        end

        @semaphore = Mutex.new

        def log_start_job()
            
                @startTime =Time.now
                @stagesTime = Hash.new
                self.log_time_of_stage_change
                self.changeStageAndSubStage("running","init")
            
        end

        def log_finish_job()
                self.log_time_of_stage_change
                self.changeStageAndSubStage("idle","idle")

                self.report_times "execution_times"

                @stagesTime = nil
                @startTime = nil
        end

        def log_time_of_stage_change()
            #Executor::logger.debug Time.now
            if (@subStage =="idle")
                @stageStartTime=Time.now
            else 
                @stagesTime[@subStage] = Time.now - @stageStartTime
                @stageStartTime=Time.now
            end
        end

        def write_data(metric, stage , start_or_finish)
            time = time = Process.clock_gettime(Process::CLOCK_MONOTONIC, :millisecond)
            time_precision ="ms"
            unless @influxdb.nil?
                data = {
                    values: {  stage: stage, workflow_stage: @stage, stage_name: start_or_finish },
                    tags:   { wfid: @wfid, hfId: @hfId, workerId: @id ,jobId: @jobId , procId: @procId ,workflow_stage: @stage}
                }
                #Executor::logger.debug "write to database #{data}"

                @influxdb.write_point(metric, data, time_precision)
            end
        end

        def report_times(metric)
            unless @influxdb.nil?
                runing_time = Time.now - @startTime

                data = {
                    values: { runing_time: runing_time, downloading_time: @stagesTime["stage_in"] , 
                    execution_time: @stagesTime["execution"] , uploading_time: @stagesTime["stage_out"]},
                    tags:   { wfid: @wfid, hfId: @hfId, workerId: @id , jobId: @jobId, procId: @procId}
                }
                #Executor::logger.debug "write to database #{data}"
                @influxdb.write_point(metric, data)

                @execution_times_running_time_gauge.set(
                    { wfid: @wfid, hfId: @hfId, workerId: @id , jobId: @jobId, procId: @procId},
                    runing_time
                )
                @execution_times_downloading_time_gauge.set(
                    { wfid: @wfid, hfId: @hfId, workerId: @id , jobId: @jobId, procId: @procId},
                    @stagesTime["stage_in"]
                )
                @execution_times_execution_time_gauge.set(
                    { wfid: @wfid, hfId: @hfId, workerId: @id , jobId: @jobId, procId: @procId},
                    @stagesTime["execution"]
                )
                @execution_times_uploading_time_gauge.set(
                    { wfid: @wfid, hfId: @hfId, workerId: @id , jobId: @jobId, procId: @procId},
                    @stagesTime["stage_out"]
                )
                @prometheus_pushGateway.add(@registry)
            end
        end

        def log_start_subStage(subStage)
 
            self.log_time_of_stage_change
            self.changeSubStage(subStage)
            
            # self.log_time_of_stage_change
            # self.changeSubStage("downloading")
        end
      end
      
  end