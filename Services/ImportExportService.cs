using ExpressBase.Common;
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
        public GetOneFromAppstoreResponse Get(GetOneFromAppStoreRequest request)
        {
            DbParameter[] Parameters = { InfraConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.Int32, request.Id) };
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery("SELECT * FROM eb_appstore WHERE id = :id", Parameters);
            return new GetOneFromAppstoreResponse
            {
                Wrapper = (AppWrapper)EbSerializers.Json_Deserialize(dt.Rows[0][7].ToString())
            };
        }
        public GetAllFromAppstoreResponse Get(GetAllFromAppStoreRequest request)
        {
            List<AppStore> _storeCollection = new List<AppStore>();
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(string.Format(@"SELECT * FROM eb_appstore WHERE user_solution_id = '{0}' AND
                                                                      eb_del='F'", request.TenantAccountId));
            foreach (EbDataRow _row in dt.Rows)
            {
                AppStore _app = new AppStore
                {
                    Id = Convert.ToInt32(_row[0]),
                    AppName = _row[1].ToString(),
                    Status = Convert.ToInt32(_row[2]),
                    Cost = Convert.ToInt32(_row[4]),
                    CreatedBy = Convert.ToInt32(_row[5]),
                    CreatedAt = Convert.ToDateTime(_row[6]),
                    Json = _row[7].ToString(),
                    Currency = _row[8].ToString()
                };
                _storeCollection.Add(_app);
            }
            return new GetAllFromAppstoreResponse { Apps = _storeCollection };
        }

        public SaveToAppStoreResponse Post(SaveToAppStoreRequest request)
        {
            using (DbConnection con = this.InfraConnectionFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql = @"INSERT INTO eb_appstore (app_name, status, user_solution_id, cost, created_by, created_at, json, currency)
                                                VALUES (:app_name, :status, :user_solution_id, :cost, :created_by, Now(), :json, :currency)";
                DbCommand cmd = InfraConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":app_name", EbDbTypes.String, request.Store.AppName));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":status", EbDbTypes.Int32, request.Store.Status));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":user_solution_id", EbDbTypes.String, request.TenantAccountId));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":cost", EbDbTypes.Int32, request.Store.Cost));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":created_by", EbDbTypes.Int32, request.UserId));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":json", EbDbTypes.Json, request.Store.Json));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":currency", EbDbTypes.String, request.Store.Currency));
                object x = cmd.ExecuteScalar();
                return new SaveToAppStoreResponse { };
            }
        }

    }
}
