using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Microsoft.Rest;
using ServiceStack;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace ExpressBase.ServiceStack.Services
{
    public class AppStoreService : EbBaseService
    {
        public AppStoreService(IEbConnectionFactory _dbf) : base(_dbf) { }

        public GetOneFromAppstoreResponse Get(GetOneFromAppStoreRequest request)
        {
            DbParameter[] Parameters = { this.InfraConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.Int32, request.Id) };
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery("SELECT * FROM eb_appstore WHERE id = :id", Parameters);
            AppWrapper _wrapper = null;
            if (dt.Rows.Count > 0)
                _wrapper = EbSerializers.Json_Deserialize<AppWrapper>(dt.Rows[0][7].ToString());
            return new GetOneFromAppstoreResponse
            {
                Wrapper = _wrapper
            };
        }

        public GetAllFromAppstoreResponse Get(GetAllFromAppStoreExternalRequest request)
        {
            List<AppStore> _storeCollection = new List<AppStore>();
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(string.Format(@"
            SELECT
	            EAS.id, app_name, status, user_solution_id, cost, created_by, created_at, json, currency, EAS.eb_del, app_type,	EAS.description, icon, solution_name, fullname
            FROM 
	            eb_appstore EAS, eb_solutions ES, eb_tenants ET
            WHERE 
                EAS.user_solution_id = ES.esolution_id AND
                ES.tenant_id = ET.id AND EAS.eb_del='F' AND
                EAS.status=2;
            ", request.SolnId));
            foreach (EbDataRow _row in dt.Rows)
            {
                AppStore _app = new AppStore
                {
                    Id = Convert.ToInt32(_row[0]),
                    Name = _row[1].ToString(),
                    Status = Convert.ToInt32(_row[2]),
                    SolutionId = _row[3].ToString(),
                    Cost = Convert.ToInt32(_row[4]),
                    CreatedBy = Convert.ToInt32(_row[5]),
                    CreatedAt = Convert.ToDateTime(_row[6]),
                    Json = _row[7].ToString(),
                    Currency = _row[8].ToString(),
                    AppType = Convert.ToInt32(_row[10]),
                    Description = _row[11].ToString(),
                    Icon = _row[12].ToString(),
                    SolutionName = _row[13].ToString(),
                    TenantName = _row[14].ToString()
                };
                _storeCollection.Add(_app);
            }
            return new GetAllFromAppstoreResponse { Apps = _storeCollection };
        }
        [Authenticate]
        public GetAllFromAppstoreResponse Get(GetAllFromAppStoreInternalRequest request)
        {
            List<AppStore> _storeCollection = new List<AppStore>();
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(string.Format(@"
            SELECT
	            EAS.id, app_name, status, user_solution_id, cost, created_by, created_at, json, currency, EAS.eb_del, app_type,	EAS.description, icon, solution_name, fullname
            FROM 
	            eb_appstore EAS, eb_solutions ES, eb_tenants ET
            WHERE 
                EAS.user_solution_id = ES.esolution_id AND
                ES.tenant_id = ET.id AND EAS.eb_del='F' AND
                EAS.status=1 AND 
                ES.tenant_id=(SELECT ES.tenant_id from eb_solutions ES where ES.esolution_id = '{0}');
            ", request.SolnId));
            foreach (EbDataRow _row in dt.Rows)
            {
                AppStore _app = new AppStore
                {
                    Id = Convert.ToInt32(_row[0]),
                    Name = _row[1].ToString(),
                    Status = Convert.ToInt32(_row[2]),
                    SolutionId = _row[3].ToString(),
                    Cost = Convert.ToInt32(_row[4]),
                    CreatedBy = Convert.ToInt32(_row[5]),
                    CreatedAt = Convert.ToDateTime(_row[6]),
                    Json = _row[7].ToString(),
                    Currency = _row[8].ToString(),
                    AppType = Convert.ToInt32(_row[10]),
                    Description = _row[11].ToString(),
                    Icon = _row[12].ToString(),
                    SolutionName = _row[13].ToString(),
                    TenantName = _row[14].ToString()
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
                string sql = @"INSERT INTO eb_appstore (app_name, status, user_solution_id, cost, created_by, created_at, json, currency, app_type, description, icon)
                                                VALUES (:app_name, :status, :user_solution_id, :cost, :created_by, Now(), :json, :currency, :app_type, :description, :icon);";
                DbCommand cmd = InfraConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":app_name", EbDbTypes.String, request.Store.Name));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":status", EbDbTypes.Int32, request.Store.Status));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":user_solution_id", EbDbTypes.String, request.SolnId));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":cost", EbDbTypes.Decimal, request.Store.Cost));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":created_by", EbDbTypes.Int32, request.UserId));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":json", EbDbTypes.Json, request.Store.Json));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":currency", EbDbTypes.String, request.Store.Currency));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":app_type", EbDbTypes.Int32, request.Store.AppType));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":description", EbDbTypes.String, request.Store.Description));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":icon", EbDbTypes.String, request.Store.Icon));
                object x = cmd.ExecuteScalar();
                return new SaveToAppStoreResponse { };
            }
        }

        public ShareToPublicResponse Post(ShareToPublicRequest request)
        {
            Log.Info("Entered ShareToPublicRequest");
            int _id;
            using (DbConnection con = this.InfraConnectionFactory.ObjectsDB.GetNewConnection())
            {
                con.Open();
                string sql;
                if (request.Store.DetailId > 0)
                    sql = @"
                            UPDATE eb_appstore_detailed 
                            SET  title = :title, is_free = :is_free, short_desc = :short_desc, tags = :tags, 
                                 detailed_desc = :detailed_desc, demo_links = :demo_links,
                                 video_links = :video_links, images = :images, pricing_desc = :pricing_desc
                            WHERE 
                                app_store_id = :app_store_id; 
                            UPDATE eb_appstore SET cost = :cost WHERE id = :app_store_id;";
                else
                    sql = @"
                        INSERT INTO eb_appstore_detailed(app_store_id, title, is_free, published_at, published_by,
								 short_desc, tags, detailed_desc, demo_links, video_links, images, pricing_desc)
                            VALUES (:app_store_id, :title, :is_free, Now(), :published_by, :short_desc, :tags,
		                            :detailed_desc, :demo_links, :video_links, :images, :pricing_desc);
                        UPDATE eb_appstore SET status = 2, cost = :cost WHERE id = :app_store_id;";
                DbCommand cmd = InfraConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":app_store_id", EbDbTypes.Int32, request.Store.Id));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":title", EbDbTypes.String, request.Store.Title));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":is_free", EbDbTypes.String, (Convert.ToInt32(request.Store.IsFree) == 1) ? "T" : "F"));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":published_by", EbDbTypes.Int32, request.UserId));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":short_desc", EbDbTypes.String, request.Store.ShortDesc));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":tags", EbDbTypes.String, request.Store.Tags));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":detailed_desc", EbDbTypes.String, request.Store.DetailedDesc));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":demo_links", EbDbTypes.String, request.Store.DemoLinks));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":video_links", EbDbTypes.String, request.Store.VideoLinks));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":images", EbDbTypes.String, request.Store.Images));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":pricing_desc", EbDbTypes.String, request.Store.PricingDesc));
                cmd.Parameters.Add(InfraConnectionFactory.ObjectsDB.GetNewParameter(":cost", EbDbTypes.Decimal, request.Store.Cost));
                _id = cmd.ExecuteNonQuery();
            }
            Log.Info("ShareToPublicRequest returning id = " + _id);
            return new ShareToPublicResponse { ReturningId = _id };
        }

        public GetAppDetailsResponse Get(GetAppDetailsRequest request)
        {
            DbParameter[] Parameters = { InfraConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.Int32, request.Id) };
            string sql = @"SELECT * FROM 
                             eb_appstore_detailed EAD , eb_appstore EA
                           WHERE
                             EA.id =:id AND
                             EAD.app_store_id = EA.id;";
            List<AppStore> _storeCollection = new List<AppStore>();
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(sql, Parameters);
            foreach (EbDataRow _row in dt.Rows)
            {
                AppStore app_detail = new AppStore
                {
                    DetailId = Convert.ToInt32(_row[0]),
                    Title = _row[1].ToString(),
                    IsFree = (_row[2].ToString() == "T") ? "1" : "2",
                    ShortDesc = _row[5].ToString(),
                    Tags = _row[6].ToString(),
                    DetailedDesc = _row[7].ToString(),
                    DemoLinks = _row[8].ToString(),
                    VideoLinks = _row[9].ToString(),
                    Images = _row[10].ToString(),
                    PricingDesc = _row[11].ToString(),
                    Cost = Math.Round(Convert.ToDecimal(_row[17]), 2)
                };
                _storeCollection.Add(app_detail);
            }
            return new GetAppDetailsResponse { StoreCollection = _storeCollection };
        }
    }
}
