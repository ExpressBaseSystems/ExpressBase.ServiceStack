using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [Authenticate]
    public class ChatbotServices : EbBaseService
    {
        public ChatbotServices(ITenantDbFactory _dbf) : base(_dbf) { }

        public CreateBotResponse Post(CreateBotRequest request)
        {
            string botid = null;
            try
            {
                using (var con = this.TenantDbFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    string sql = "SELECT * FROM eb_createbot(@solid, @name, @url, @welcome_msg, @uid, @botid)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@solid", System.Data.DbType.String, request.SolutionId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@name", System.Data.DbType.String, request.BotName));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@url", System.Data.DbType.String, request.WebURL));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@welcome_msg", System.Data.DbType.String, request.WelcomeMsg));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@uid", System.Data.DbType.Int32, request.UserId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@botid", System.Data.DbType.Int32, (request.BotId != null)? request.BotId : "0"));

                    botid = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {

            }
            return new CreateBotResponse() { BotId = botid };
        }

        List<System.Data.Common.DbParameter> parameters = new List<System.Data.Common.DbParameter>();

        [CompressResponse]
        public object Get(BotListRequest request)
        {
            List<ChatBot> res = new List<ChatBot>();
            string sql = @"
SELECT 
    id,
	name, 
	url, 
	botid, 
	(SELECT firstname FROM eb_users WHERE id = eb_bots.created_by) AS created_by, 
	created_at, 
	(SELECT firstname FROM eb_users WHERE id = eb_bots.modified_by) AS modified_by, 
	modified_at, welcome_msg 
FROM 
	eb_bots 
WHERE 
	solution_id = @solid;";
            parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@solid", System.Data.DbType.Int32, 100));//request.SolutionId));
            var dt = this.TenantDbFactory.ObjectsDB.DoQuery(sql, parameters.ToArray());

            foreach (var dr in dt.Rows)
            {
                ChatBot bot = new ChatBot
                {
                    BotId = dr[0].ToString(),
                    Name = dr[1].ToString(),
                    WebsiteURL = dr[2].ToString(),
                    ChatId = dr[3].ToString(),
                    CreatedBy = dr[4].ToString(),
                    CreatedAt = Convert.ToDateTime(dr[5]),
                    LastModifiedBy = dr[6].ToString(),
                    LastModifiedAt = Convert.ToDateTime(dr[7]),
                    WelcomeMsg = dr[8].ToString()
                };
                res.Add(bot);
            }
            return new BotListResponse { Data = res };
        }

    }
}
