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
                    string sql = "SELECT eb_createbot(@solid, @name, @url, @welcome_msg)";
                    cmd = this.TenantDbFactory.ObjectsDB.GetNewCommand(con, sql);
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@solid", System.Data.DbType.String, request.SolutionId));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@name", System.Data.DbType.String, request.BotName));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@url", System.Data.DbType.String, request.WebURL));
                    cmd.Parameters.Add(this.TenantDbFactory.ObjectsDB.GetNewParameter("@welcome_msg", System.Data.DbType.String, request.WelcomeMsg));

                    botid = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {

            }
            return new CreateBotResponse(){ BotId = botid } ;
        }
    }
}
