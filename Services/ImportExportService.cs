using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class ImportExportService : EbBaseService
    {
        public ImportExportService(IEbConnectionFactory _dbf) : base(_dbf) { }

        public SaveToAppStoreResponse Post(SaveToAppStoreRequest request)
        {
            using (var con = this.InfraConnectionFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = @"INSERT INTO eb_appstore (app_name, status, user_tenant_acc_id, cost, created_by, created_at, json, currency)
                                                VALUES (:app_name, :status, :user_tenant_acc_id, :cost, :created_by, Now(), :json, :currency)";
                DbCommand cmd = EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
                cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":app_name", EbDbTypes.String, request.AppName));
                cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":status", EbDbTypes.Int32, request.Status));
                cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":user_tenant_acc_id", EbDbTypes.String, request.TenantAccountId));
                cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":cost", EbDbTypes.Int32, request.Cost));
                cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":created_by", EbDbTypes.Int32, request.UserId));
                cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":json", EbDbTypes.Json, request.Json));
                cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter(":currency", EbDbTypes.Json, request.Currency));
                var x= cmd.ExecuteScalar();
                return new SaveToAppStoreResponse { };
            }
        }

    }
}
