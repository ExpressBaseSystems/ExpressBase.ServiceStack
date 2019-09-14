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
            EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(@"SELECT title, json, status FROM eb_appstore s , eb_appstore_detailed d
                                                                        WHERE s.id = :id and s.id = d.app_store_id", Parameters);
            AppWrapper _wrapper = null;
            if (dt.Rows.Count > 0)
            {
                _wrapper = EbSerializers.Json_Deserialize<AppWrapper>(dt.Rows[0]["json"].ToString());
                _wrapper.Title = dt.Rows[0]["title"].ToString();
                _wrapper.IsPublic = (((int)dt.Rows[0]["status"]) == 2) ? true : false;
            }
            else
                Console.WriteLine("Could't retrieve app from table eb_appstore. app id:" + request.Id);
            return new GetOneFromAppstoreResponse
            {
                Wrapper = _wrapper
            };
        }

        public GetAppStoreDetailedResponse Get(GetAppStoreDetailedRequest request)
        {
            GetAppStoreDetailedResponse resp = new GetAppStoreDetailedResponse();
            string query = @"SELECT 
	                            EAS.app_name,EAS.cost,EAS.created_by,EAS.created_at,EAS.currency,EAS.app_type,
	                            EAS.icon,EASD.*
                            FROM 
	                            eb_appstore EAS 
                            INNER JOIN 
	                            eb_appstore_detailed EASD 
                            ON 
	                            EAS.id = EASD.app_store_id
                            WHERE
	                            EAS.id = :id;";
            try
            {
                DbParameter[] Parameters = {
                    this.InfraConnectionFactory.ObjectsDB.GetNewParameter(":id", EbDbTypes.Int32, request.Id)
                };
                EbDataTable dt = this.InfraConnectionFactory.ObjectsDB.DoQuery(query, Parameters);
                if (dt.Rows.Count > 0)
                {
                    EbDataRow _row = dt.Rows[0];
                    resp.Store.Cost = Convert.ToInt32(_row["cost"]);
                    resp.Store.Title = _row["title"].ToString();
                    resp.Store.CreatedAt = Convert.ToDateTime(_row["created_at"]);
                    resp.Store.Currency = _row["currency"].ToString();
                    resp.Store.AppType = Convert.ToInt32(_row["app_type"]);
                    resp.Store.Icon = _row["icon"].ToString();
                    resp.Store.ShortDesc = _row["short_desc"].ToString();
                    resp.Store.Tags = _row["tags"].ToString();
                    resp.Store.IsFree = _row["is_free"].ToString();
                    resp.Store.DetailedDesc = _row["detailed_desc"].ToString();
                    resp.Store.DemoLinks = _row["demo_links"].ToString();
                    resp.Store.VideoLinks = _row["video_links"].ToString();
                    resp.Store.Images = _row["images"].ToString();
                    resp.Store.PricingDesc = _row["pricing_desc"].ToString();
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return resp;
        }

        public GetAllFromAppstoreResponse Get(GetAllFromAppStoreExternalRequest request)
        {
            GetAllFromAppstoreResponse resp = new GetAllFromAppstoreResponse();
            string query = @"SELECT
	                            EAS.id, EAD.title,cost, created_by, created_at, currency, app_type,EAS.description, icon, 
	                            EAD.images,EAD.detailed_desc, is_free, short_desc,tags, title
                            FROM 
	                            eb_appstore EAS, eb_appstore_detailed EAD
                            WHERE 
                            EAS.eb_del='F' AND EAD.app_store_id = EAS.id AND EAS.status = 2;";
            try
            {
                EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(query);
                foreach (EbDataRow _row in dt.Rows)
                {
                    resp.Apps.Add(new AppStore
                    {
                        Id = Convert.ToInt32(_row["id"]),
                        Cost = Convert.ToInt32(_row["cost"]),
                        CreatedBy = Convert.ToInt32(_row["created_by"]),
                        CreatedAt = Convert.ToDateTime(_row["created_at"]),
                        Currency = _row["currency"].ToString(),
                        AppType = Convert.ToInt32(_row["app_type"]),
                        Description = _row["description"].ToString(),
                        Icon = _row["icon"].ToString(),
                        Images = _row["images"].ToString(),
                        DetailedDesc = _row["detailed_desc"].ToString(),
                        IsFree = _row["is_free"].ToString(),
                        ShortDesc = _row["short_desc"].ToString(),
                        Tags = _row["tags"].ToString(),
                        Title = _row["title"].ToString(),
                    });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return resp;
        }

        [Authenticate]
        public GetAllFromAppstoreResponse Get(GetAllFromAppStoreInternalRequest request)
        {
            GetAllFromAppstoreResponse resp = new GetAllFromAppstoreResponse();
            List<DbParameter> parameters = new List<DbParameter>();
            string q = string.Empty;
            try
            {
                if (request.WhichConsole == RoutingConstants.TC)
                {
                    q = @"SELECT 
	                    id,app_name,user_solution_id,created_by, created_at,description,app_type,icon,status
                    FROM
	                    eb_appstore
                    WHERE
	                    eb_del = 'F' AND
	                    user_solution_id = ANY(SELECT isolution_id FROM eb_solutions WHERE tenant_id = :tenantid);";
                    parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("tenantid", EbDbTypes.Int32, request.UserId));
                }
                else
                {
                    q = @"SELECT 
	                    id,app_name,user_solution_id,created_by, created_at,description,app_type,icon,status
                    FROM
	                    eb_appstore
                    WHERE
	                    eb_del = 'F' AND
	                    user_solution_id = :isolutionid ;";
                    parameters.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("isolutionid", EbDbTypes.String, request.SolnId));
                }

                EbDataTable dt = InfraConnectionFactory.ObjectsDB.DoQuery(q, parameters.ToArray());
                foreach (EbDataRow _row in dt.Rows)
                {
                    int status = Convert.ToInt32(_row["status"]);
                    if (status == 1)
                    {
                        resp.Apps.Add(new AppStore
                        {
                            Id = Convert.ToInt32(_row["id"]),
                            Name = _row["app_name"].ToString(),
                            SolutionId = _row["user_solution_id"].ToString(),
                            CreatedBy = Convert.ToInt32(_row["created_by"]),
                            CreatedAt = Convert.ToDateTime(_row["created_at"]),
                            AppType = Convert.ToInt32(_row["app_type"]),
                            Description = _row["description"].ToString(),
                            Icon = _row["icon"].ToString(),
                        });
                    }
                    else if (status == 2)
                    {
                        resp.PublicApps.Add(new AppStore
                        {
                            Id = Convert.ToInt32(_row["id"]),
                            Name = _row["app_name"].ToString(),
                            SolutionId = _row["user_solution_id"].ToString(),
                            CreatedBy = Convert.ToInt32(_row["created_by"]),
                            CreatedAt = Convert.ToDateTime(_row["created_at"]),
                            AppType = Convert.ToInt32(_row["app_type"]),
                            Description = _row["description"].ToString(),
                            Icon = _row["icon"].ToString(),
                        });
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }

            return resp;
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

        public AppAndsolutionInfoResponse Get(AppAndsolutionInfoRequest request)
        {
            AppAndsolutionInfoResponse resp = new AppAndsolutionInfoResponse();
            try
            {
                string q = @"SELECT solution_name,isolution_id,esolution_id FROM eb_solutions WHERE eb_del = 'F' AND tenant_id = :tid;
                        SELECT 
	                        EAS.app_name,EAS.cost,EAS.created_by,EAS.created_at,EAS.currency,EAS.app_type,
	                        EAS.icon,EASD.*
                        FROM 
	                        eb_appstore EAS 
                        INNER JOIN 
	                        eb_appstore_detailed EASD 
                        ON 
	                        EAS.id = EASD.app_store_id
                        WHERE
	                        EAS.id = :appid;";

                DbParameter[] parameters =
                {
                    this.InfraConnectionFactory.DataDB.GetNewParameter("tid", EbDbTypes.Int32, request.UserId),
                    this.InfraConnectionFactory.DataDB.GetNewParameter("appid", EbDbTypes.Int32, request.AppId)
                };

                EbDataSet dt = this.InfraConnectionFactory.DataDB.DoQueries(q, parameters);
                foreach (EbDataRow _row in dt.Tables[0].Rows)
                {
                    resp.Solutions.Add(new EbSolutionsWrapper
                    {
                        SolutionName = _row["solution_name"].ToString(),
                        EsolutionId = _row["esolution_id"].ToString(),
                        IsolutionId = _row["isolution_id"].ToString()
                    });
                }

                resp.AppData.Title = dt.Tables[1].Rows[0]["title"].ToString();
                resp.AppData.AppType = Convert.ToInt32(dt.Tables[1].Rows[0]["app_type"]);
                resp.AppData.ShortDesc = dt.Tables[1].Rows[0]["short_desc"].ToString();
                resp.AppData.Tags = dt.Tables[1].Rows[0]["tags"].ToString();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return resp;
        }
    }
}
