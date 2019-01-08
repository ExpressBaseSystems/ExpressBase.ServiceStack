using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
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
    public class SchedulerServices : EbBaseService
    {
        public SchedulerServices(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _mqp, _mqc) { }

        public SchedulerMQResponse Post(SchedulerMQRequest request)
        {
            //using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
            //{
            //    con.Open();
            //    string sql = @"INSERT INTO eb_schedules(task, created_by, created_at, eb_del)
            //    VALUES(:task, :created_by, NOW(), 'F')";
            //    DbParameter[] parameters = { EbConnectionFactory.DataDB.GetNewParameter("task", EbDbTypes.Json,EbSerializers.Json_Serialize(request.Task)),
            //   EbConnectionFactory.DataDB.GetNewParameter("created_by", EbDbTypes.Int32,  request.Task.JobArgs.UserId) };
            //    var dt = EbConnectionFactory.DataDB.DoNonQuery(sql, parameters);
            //}
            MessageProducer3.Publish(new ScheduleRequest { Task = request.Task });
            return null;
        }

        public UnschedulerMQResponse Post(UnschedulerMQRequest request)
        {
            UnschedulerMQResponse res = new UnschedulerMQResponse();
            MessageProducer3.Publish(new UnscheduleRequest { TriggerKey = request.TriggerKey });
            return res;

        }

        public GetAllUsersResponse Get(GetAllUsersRequest request)
        {
            GetAllUsersResponse res = new GetAllUsersResponse();
            using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string sql = @"SELECT id, fullname FROM eb_users WHERE statusid = 0 ;
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
            }
            return res;
        }

        public GetUserEmailsResponse Get(GetUserEmailsRequest request)
        {
            GetUserEmailsResponse res = new GetUserEmailsResponse();
            using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string sql = @"SELECT id, email FROM eb_users WHERE id = ANY
                             (string_to_array(:userids,',')::int[]);
                           SELECT distinct id, email FROM eb_users WHERE id = ANY(SELECT userid FROM eb_user2usergroup WHERE 
                                groupid = ANY(string_to_array(:groupids,',')::int[])) ;";
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
            }
            return res;
        }

        public GetSchedulesOfSolutionResponse Get(GetSchedulesOfSolutionRequest request)
        {
            GetSchedulesOfSolutionResponse resp = new GetSchedulesOfSolutionResponse();
            List<EbSchedule> scheduleList = new List<EbSchedule>();
            using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string sql = @"SELECT * FROM eb_schedules ES ,eb_users EU
                               WHERE EU.id =ES.created_by;";
                EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(sql);
                foreach (EbDataRow dr in dt.Tables[0].Rows)
                {
                    EbSchedule sch = new EbSchedule
                    {
                        Id = Convert.ToInt32(dr[0]),
                        Task = EbSerializers.Json_Deserialize<EbTask>(dr[1].ToString()),
                        CreatedBy = dr[33].ToString(),
                        CreatedAt = Convert.ToDateTime(dr[3]),
                        JobKey = dr[5].ToString(),
                        TriggerKey = dr[6].ToString(),
                        Status = (ScheduleStatuses)Convert.ToInt32(dr[7])
                    };
                    scheduleList.Add(sch);
                }
                resp.Schedules = scheduleList;
            }
            return resp;
        }
    }

    [Restrict(InternalOnly = true)]
    class UpdateSolutionSchedulesServices : EbMqBaseService
    {
        public UpdateSolutionSchedulesServices(IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_mqp, _mqc) { }

        public SchedulerMQResponse Post(UpdateSolutionSchedulesRequest request)
        {
            EbConnectionFactory _ebConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
            using (var con = _ebConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string sql = @"INSERT INTO eb_schedules(task, created_by, created_at, eb_del, jobkey, triggerkey, status)
                VALUES(:task, :created_by, NOW(), 'F', :jobkey, :triggerkey, :status)";
                DbParameter[] parameters = { _ebConnectionFactory.DataDB.GetNewParameter("task", EbDbTypes.Json,EbSerializers.Json_Serialize(request.Task)),
               _ebConnectionFactory.DataDB.GetNewParameter("created_by", EbDbTypes.Int32,  request.Task.JobArgs.UserId),
               _ebConnectionFactory.DataDB.GetNewParameter("jobkey", EbDbTypes.String,  request.JobKey),
               _ebConnectionFactory.DataDB.GetNewParameter("triggerkey", EbDbTypes.String,  request.TriggerKey),
                _ebConnectionFactory.DataDB.GetNewParameter("status", EbDbTypes.Int32, (int)request.Status) };
                var dt = _ebConnectionFactory.DataDB.DoNonQuery(sql, parameters);
            }
            return null;
        }


    }
}
