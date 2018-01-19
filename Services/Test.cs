using ExpressBase.Common.Data;
using ExpressBase.Common.Data.OracleDB;
using ExpressBase.Objects.ServiceStack_Artifacts;
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
            var con = TenantDbFactory.DataDB.GetNewConnection();
            con.Open();
            string sql = "INSERT INTO testtb (name) VALUES ('BINI')";

            //var cmd = c.GetNewCommand(con, sql);
            //cmd.ExecuteNonQuery();

            
            return null;
        }
    }
}
