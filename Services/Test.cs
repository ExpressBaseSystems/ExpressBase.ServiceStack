using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using ServiceStack.Auth;
using ServiceStack.Logging;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class Test : EbBaseService
    {
        public Test(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _mqp, _mqc) { }

        public TestResponse Any(TestRequest request)
        {
            ILog log = LogManager.GetLogger(GetType());
            var con = EbConnectionFactory.DataDB.GetNewConnection();
            log.Info("Connection");
            con.Open();
            log.Info(".............." + con + "Connection Opened");
            // string sql = "INSERT INTO testtb (name) VALUES ('BINI')";

            try
            {
                string sql1 = "INSERT INTO test_tbl(Name,number) VALUES ('ref',eb_currval(test_tbl_id_seq));";
                //string sql2 = "INSERT INTO test_tbl(name,number) VALUES ('ref',123);INSERT INTO test_tbl(name,number) VALUES ('ref',eb_currval(test_tbl_id_seq));";
                var cmd = con.CreateCommand();
                cmd.CommandText = sql1;
                cmd.ExecuteNonQuery();
            }
            catch
            {
            }

            return null;
        } 
    }

    //[Authenticate]
    //public class ReqstarsService : Service
    //{
    //    public object Get(ApiTestReq request)
    //    {
    //        var authProvider = (ApiKeyAuthProvider)AuthenticateService.GetAuthProvider(ApiKeyAuthProvider.Name);

    //        var auth_api = (ApiKeyAuthProvider)AuthenticateService.GetAuthProvider(ApiKeyAuthProvider.Name);

    //        var authRepo = TryResolve<IManageApiKeys>();

    //        List<ApiKey> apiKeys = auth_api.GenerateNewApiKeys("hairocraft_stagging:hairocraft123@gmail.com:uc");

    //        authRepo.StoreAll(apiKeys);

    //        return new UnniTest();
    //    }
    //}
}
