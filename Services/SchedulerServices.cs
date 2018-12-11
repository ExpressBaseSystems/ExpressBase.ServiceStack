using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
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
        public SchedulerServices(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc):base(_dbf, _mqp, _mqc) { }

        public SchedulerMQResponse Post(SchedulerMQRequest request)
        {
            using (var con = this.EbConnectionFactory.DataDB.GetNewConnection())
            {
                con.Open();
                string sql = @"INSERT INTO eb_schedules(task, created_by, created_at, eb_del)
                VALUES(:task, :created_by, NOW(), 'F')";
                DbParameter[] parameters = { EbConnectionFactory.DataDB.GetNewParameter("task", EbDbTypes.Json,EbSerializers.Json_Serialize(request.Task)),
               EbConnectionFactory.DataDB.GetNewParameter("created_by", EbDbTypes.Int32,  request.Task.JobArgs.UserId) };
                var dt = EbConnectionFactory.DataDB.DoNonQuery(sql, parameters);
            }
            MessageProducer3.Publish(new SchedulerRequest { Task = request.Task });
            return null;
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
            return res ;
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
    }
}
