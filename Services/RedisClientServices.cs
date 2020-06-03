using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using ExpressBase.Common;
using ServiceStack;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class RedisClientServices : EbBaseService
    {
        public RedisClientServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public LogRedisInsertResponse Post(LogRedisInsertRequest request)
        {
            string query = @"INSERT INTO eb_redis_logs (changed_by, operation, changed_at, prev_value, new_value, soln_id, key) VALUES(:usr, :opn, NOW(), :prev, :new,
                            :sln, :key);";
            DbParameter[] parameters = {
                     InfraConnectionFactory.ObjectsDB.GetNewParameter("usr", EbDbTypes.Int32, request.UserId),
                InfraConnectionFactory.ObjectsDB.GetNewParameter("opn", EbDbTypes.Int32, request.Operation),
                InfraConnectionFactory.ObjectsDB.GetNewParameter("prev", EbDbTypes.String, request.PreviousValue),
                InfraConnectionFactory.ObjectsDB.GetNewParameter("new", EbDbTypes.String, request.NewValue),
                InfraConnectionFactory.ObjectsDB.GetNewParameter("sln", EbDbTypes.Int32, request.SolutionId),
                InfraConnectionFactory.ObjectsDB.GetNewParameter("key", EbDbTypes.String, request.Key)
            };
            this.InfraConnectionFactory.ObjectsDB.DoNonQuery(query, parameters);
            return new LogRedisInsertResponse();
        }

        public LogRedisGetResponse Get(LogRedisGetRequest request)
        {
            List<EbRedisLogs> r_logs = new List<EbRedisLogs>();
            List<DbParameter> parameters = new List<DbParameter>();
            string query = @"SELECT changed_by, operation, changed_at, soln_id, key, id FROM eb_redis_logs WHERE soln_id = :slnid order by changed_at desc";
            parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter("slnid", EbDbTypes.Int32, request.SolutionId));
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(query, parameters.ToArray());
            foreach (var item in dt.Rows)
            {
                EbRedisLogs eb = new EbRedisLogs
                {
                    LogId = Convert.ToInt32(item[5]),
                    ChangedBy = Convert.ToInt32(item[0]),
                    ChangedAt = Convert.ToDateTime(item[2]),
                    Operation = Enum.GetName(typeof(RedisOperations), item[1]),
                    Key = (item[4]).ToString()
                };
                r_logs.Add(eb);
            }
            return new LogRedisGetResponse { Logs = r_logs };
        }

        public LogRedisViewChangesResponse Get(LogRedisViewChangesRequest request)
        {

            //EbRedisLogValues ebRedisLogValues = new EbRedisLogValues();
            List<DbParameter> parameters = new List<DbParameter>();
            string query = @"SELECT prev_value, new_value FROM eb_redis_logs WHERE id = :logid";
            parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter("logid", EbDbTypes.Int32, request.LogId));
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(query, parameters.ToArray());
            EbRedisLogValues logValues = new EbRedisLogValues
            {
                Prev_val = dt.Rows[0][0].ToString(),
                New_val = dt.Rows[0][1].ToString()

            };
            return new LogRedisViewChangesResponse { RedisLogValues = logValues };
        }

        public RedisGroupDetailsResponse Get(RedisGetGroupDetails request)
        {
            Dictionary<string, List<EbRedisGroupDetails>> grpdict = new Dictionary<string, List<EbRedisGroupDetails>>();
            string qry = @"select EO.obj_type, EO.display_name, EV.refid, EV.version_num FROM eb_objects EO , eb_objects_ver EV WHERE EO.id = EV.eb_objects_id AND (COALESCE(EO.eb_del, 'F')= 'F') order by EO.display_name";
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(qry);
            List<EbRedisGroupDetails> l0 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> l1 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> l2 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> l3 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> l4 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> l5 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> L12 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> L14 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> L15 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> L16 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> L17 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> L18 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> L19 = new List<EbRedisGroupDetails>();
            List<EbRedisGroupDetails> L20 = new List<EbRedisGroupDetails>();
            foreach (var item in dt.Rows)
            {
                EbRedisGroupDetails ob = new EbRedisGroupDetails
                {
                    Obj_Type = Convert.ToInt32(item[0]),
                    Disp_Name = Convert.ToString(item[1]),
                    Refid = Convert.ToString(item[2]),
                    Version = Convert.ToString(item[3])

                };

                if (ob.Obj_Type == 0) l0.Add(ob);
                else if (ob.Obj_Type == 1) l1.Add(ob);
                else if (ob.Obj_Type == 2) l2.Add(ob);
                else if (ob.Obj_Type == 3) l3.Add(ob);
                else if (ob.Obj_Type == 4) l4.Add(ob);
                else if (ob.Obj_Type == 5) l5.Add(ob);
                else if (ob.Obj_Type == 12) L12.Add(ob);
                else if (ob.Obj_Type == 14) L14.Add(ob);
                else if (ob.Obj_Type == 15) L15.Add(ob);
                else if (ob.Obj_Type == 16) L16.Add(ob);
                else if (ob.Obj_Type == 17) L17.Add(ob);
                else if (ob.Obj_Type == 18) L18.Add(ob);
                else if (ob.Obj_Type == 19) L19.Add(ob);
                else if (ob.Obj_Type == 20) L20.Add(ob);
            }
            grpdict.Add("Web Forms", l0);
            grpdict.Add("Display Block", l1);
            grpdict.Add("Data Readers", l2);
            grpdict.Add("Reports", l3);
            grpdict.Add("Data Writers", l4);
            grpdict.Add("Sql Functions", l5);
            grpdict.Add("Filter Dialogs", L12);
            grpdict.Add("User Controls", L14);
            grpdict.Add("Email Builders", L15);
            grpdict.Add("Table Visualizations", L16);
            grpdict.Add("Chart Visualizations", L17);
            grpdict.Add("Bot Forms", L18);
            grpdict.Add("Sms Builders", L19);
            grpdict.Add("Api Builders", L20);
            return new RedisGroupDetailsResponse { GroupsDict = grpdict };
        }
    }
}

