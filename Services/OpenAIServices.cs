using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Common.ServiceClients;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class OpenAIServices : EbBaseService
    {
        public OpenAIServices(IEbConnectionFactory _dbf) : base(_dbf) { }
        public OpenAISessionStoreResponse Post(OpenAISessionStoreRequest request)
        {
            OpenAISessionStoreResponse resp = new OpenAISessionStoreResponse();
            try
            {
                string query = $@"INSERT INTO eb_openai_logs(session_id, chat_heading, eb_created_by, eb_created_at)
                                VALUES(:sessionid, :chatheading, :by, {this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}) RETURNING id;";
                DbParameter[] parameters =
                {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("sessionid", EbDbTypes.String, request.SessionId),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("chatheading", EbDbTypes.String, request.ChatHeading),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("by", EbDbTypes.Int32, request.UserId)
                };
                resp.Id = this.EbConnectionFactory.DataDB.ExecuteScalar<Int32>(query, parameters);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return resp;
        }

        public ChatHistoryResponse Get(OpenAISessionStoreRequest request)
        {
            string sql = @"SELECT id, session_id, chat_heading
                        FROM eb_openai_logs
                        ORDER BY eb_created_at;";

            var ds = this.EbConnectionFactory.ObjectsDB.DoQueries(sql);

            Dictionary<int, SessionObject> sessionColl = new Dictionary<int,  SessionObject>();

            foreach (EbDataRow dr in ds.Tables[0].Rows)
            {
                var id = Convert.ToInt32(dr[0]);
                if (!sessionColl.Keys.Contains<int>(id))
                {
                    sessionColl.Add(id, new SessionObject { SessionId = dr[1].ToString(), ChatHeading = dr[2].ToString() });
                }
            }

            return new ChatHistoryResponse { Sessions = sessionColl};
        }


    }
}
