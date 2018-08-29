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
        public Test(IEbConnectionFactory _dbf) : base(_dbf) { }
        public TestResponse Any(TestRequest request)
        {
            ILog log = LogManager.GetLogger(GetType());
            var con = EbConnectionFactory.DataDB.GetNewConnection();
            log.Info("Connection");
            con.Open();
            log.Info(".............."+con+"Connection Opened");
            // string sql = "INSERT INTO testtb (name) VALUES ('BINI')";

            try
            {
                string sql1 = "INSERT INTO test_tbl(Name,number) VALUES ('ref',eb_currval(test_tbl_id_seq));";
                //string sql2 = "INSERT INTO test_tbl(name,number) VALUES ('ref',123);INSERT INTO test_tbl(name,number) VALUES ('ref',eb_currval(test_tbl_id_seq));";
                var cmd = con.CreateCommand();
                cmd.CommandText = sql1;
                cmd.ExecuteNonQuery();
            }
            catch (Exception e)
            { }
            
            return null;
        }
    }
}
