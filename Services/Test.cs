using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack.Logging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class Test : EbBaseService
    {
        public Test(ITenantDbFactory _dbf) : base(_dbf) { }
        public TestResponse Any(TestRequest request)
        {
            ILog log = LogManager.GetLogger(GetType());
            var con = TenantDbFactory.DataDB.GetNewConnection();
            log.Info("Connection");
            con.Open();
            log.Info(".............."+con+"Connection Opened");
            string sql = "INSERT INTO testtb (name) VALUES ('BINI')";

            //var cmd = c.GetNewCommand(con, sql);
            //cmd.ExecuteNonQuery();

            
            return null;
        }
    }
}
