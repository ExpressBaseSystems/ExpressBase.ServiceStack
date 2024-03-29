﻿using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Common.Connections;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Scheduler.Jobs;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class SchedulerServices : EbBaseService
    {
        public SchedulerServices(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _mqp, _mqc) { }

        public ScheduleMQResponse Post(ScheduleMQRequest request)
        {
            MessageProducer3.Publish(new ScheduleRequest { Task = request.Task });
            return null;
        }

        public UnscheduleMQResponse Post(UnscheduleMQRequest request)
        {
            UnscheduleMQResponse res = new UnscheduleMQResponse();
            MessageProducer3.Publish(new UnscheduleRequest { TriggerKey = request.TriggerKey });
            return res;

        }
        public RescheduleMQResponse Post(RescheduleMQRequest request)
        {
            RescheduleMQResponse res = new RescheduleMQResponse();

            MessageProducer3.Publish(new RescheduleRequest { Task = request.Task, JobKey = request.JobKey, TriggerKey = request.TriggerKey });

            EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            string sql = "UPDATE eb_schedules SET task = @task, name=@name WHERE id = @id";
            DbParameter[] parameters = {
                    _ebConnectionFactory.DataDB.GetNewParameter("task", EbDbTypes.Json,EbSerializers.Json_Serialize(request.Task)),
                    _ebConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32,request.Id),
                    _ebConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String,  request.Task.Name)};
            var r = _ebConnectionFactory.DataDB.DoNonQuery(sql, parameters);
            return res;
        }

        public DeleteJobMQResponse Post(DeleteJobMQRequest request)
        {
            DeleteJobMQResponse res = new DeleteJobMQResponse();
            MessageProducer3.Publish(new DeleteJobRequest { JobKey = request.JobKey });

            EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

            string sql = "UPDATE eb_schedules SET status = @stat WHERE id = @id";
            DbParameter[] parameters = {
                    _ebConnectionFactory.DataDB.GetNewParameter("stat", EbDbTypes.Int32,(int)ScheduleStatuses.Deleted),
                    _ebConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32,request.Id) };
            var r = _ebConnectionFactory.DataDB.DoNonQuery(sql, parameters);
            return res;
        }
        public GetAllUsersResponse Get(GetAllUsersRequest request)
        {
            GetAllUsersResponse res = new GetAllUsersResponse();

            string sql = @"SELECT id, fullname FROM eb_users WHERE statusid = 0 AND eb_del ='F';
                             SELECT id, name FROM eb_usergroup WHERE eb_del ='F'";
            var dt = this.EbConnectionFactory.DataDB.DoQueries(sql);

            Dictionary<int, string> Users = new Dictionary<int, string>();
            Dictionary<int, string> UserGroups = new Dictionary<int, string>();
            foreach (EbDataRow dr in dt.Tables[0].Rows)
            {
                Users[Convert.ToInt32(dr[0])] = dr[1].ToString();
            }
            foreach (EbDataRow dr in dt.Tables[1].Rows)
            {
                UserGroups[Convert.ToInt32(dr[0])] = dr[1].ToString();
            }
            res.Users = Users;
            res.UserGroups = UserGroups;

            //Slack user and group call

            res.SlackUsers = this.EbConnectionFactory.ChatConnection.GetAllUsers();
            res.SlackChannels = this.EbConnectionFactory.ChatConnection.GetAllGroup();

            return res;
        }

        public GetAllUsersResponse Get(GetAllSlackRequest request)
        {
            GetAllUsersResponse res = new GetAllUsersResponse();

            return res;
        }

        public GetUserEmailsResponse Get(GetUserEmailsRequest request)
        {
            GetUserEmailsResponse res = new GetUserEmailsResponse();

            string sql = EbConnectionFactory.DataDB.EB_GETUSEREMAILS;
            DbParameter[] parameters = { EbConnectionFactory.DataDB.GetNewParameter("userids", EbDbTypes.String, (request.UserIds==null)?string.Empty:request.UserIds),
               EbConnectionFactory.DataDB.GetNewParameter("groupids", EbDbTypes.String, ( request.UserGroupIds == null)?string.Empty:request.UserGroupIds) };

            var dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);
            Dictionary<int, string> Users = new Dictionary<int, string>();
            Dictionary<int, string> UserGroups = new Dictionary<int, string>();
            foreach (EbDataRow dr in dt.Tables[0].Rows)
            {
                Users[Convert.ToInt32(dr[0])] = dr[1].ToString();
            }
            foreach (EbDataRow dr in dt.Tables[1].Rows)
            {
                UserGroups[Convert.ToInt32(dr[0])] = dr[1].ToString();
            }
            res.UserEmails = Users;
            res.UserGroupEmails = UserGroups;

            return res;
        }

        public GetSchedulesOfSolutionResponse Get(GetSchedulesOfSolutionRequest request)
        {
            GetSchedulesOfSolutionResponse resp = new GetSchedulesOfSolutionResponse();
            List<EbSchedule> scheduleList = new List<EbSchedule>();
            string sql = "";

            if (request.ObjectId > 0)
            {
                sql = @"SELECT * FROM eb_schedules ES ,eb_users EU
                               WHERE EU.id = ES.created_by
                               AND obj_id = @obj_id AND status != 3
                                ORDER BY ES.id;";
            }
            else if (request.ObjectId == -1)
            {
                sql = @"SELECT * FROM eb_schedules ES ,eb_users EU
                               WHERE EU.id = ES.created_by AND status != 3
                               ORDER BY ES.id;";
            }

            DbParameter[] parameters = {
                    EbConnectionFactory.DataDB.GetNewParameter("obj_id", EbDbTypes.Int32,request.ObjectId)
                };
            EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(sql, parameters);
            foreach (EbDataRow dr in dt.Tables[0].Rows)
            {
                EbSchedule sch = new EbSchedule
                {
                    Id = Convert.ToInt32(dr[0]),
                    Task = EbSerializers.Json_Deserialize<EbTask>(dr[1].ToString()),
                    CreatedBy = dr[34].ToString(),
                    CreatedAt = Convert.ToDateTime(dr[3]),
                    JobKey = dr[5].ToString(),
                    TriggerKey = dr[6].ToString(),
                    Status = (ScheduleStatuses)Convert.ToInt32(dr[7]),
                    Name = dr[9].ToString()
                };
                scheduleList.Add(sch);
            }
            resp.Schedules = scheduleList;

            return resp;
        }
    }

    [Restrict(InternalOnly = true)]
    class SchedulesAndSolutionServices : EbMqBaseService
    {
        public SchedulesAndSolutionServices(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        public ScheduleMQResponse Post(AddSchedulesToSolutionRequest request)
        {
            EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);

            string sql = @"INSERT INTO eb_schedules(task, created_by, created_at, eb_del, jobkey, triggerkey, status, obj_id, name)
                VALUES(@task, @created_by, NOW(), 'F', @jobkey, @triggerkey, @status, @obj_id, @name)";
            DbParameter[] parameters = { _ebConnectionFactory.DataDB.GetNewParameter("task", EbDbTypes.Json,EbSerializers.Json_Serialize(request.Task)),
               _ebConnectionFactory.DataDB.GetNewParameter("created_by", EbDbTypes.Int32,  request.Task.JobArgs.UserId),
               _ebConnectionFactory.DataDB.GetNewParameter("jobkey", EbDbTypes.String,  request.JobKey),
               _ebConnectionFactory.DataDB.GetNewParameter("triggerkey", EbDbTypes.String,  request.TriggerKey),
                _ebConnectionFactory.DataDB.GetNewParameter("status", EbDbTypes.Int32, (int)request.Status) ,
                _ebConnectionFactory.DataDB.GetNewParameter("obj_id", EbDbTypes.Int32, (int)request.ObjId),
               _ebConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String,  request.Name)};
            var dt = _ebConnectionFactory.DataDB.DoNonQuery(sql, parameters);

            return null;
        }
    }
}
